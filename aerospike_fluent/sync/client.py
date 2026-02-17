# Copyright 2025-2026 Aerospike, Inc.
#
# Portions may be licensed to Aerospike, Inc. under one or more contributor
# license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""SyncFluentClient - Synchronous wrapper for the Aerospike Fluent API."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
import threading
from typing import List, Optional, Union, overload

from aerospike_async import ClientPolicy, Key
from aerospike_async.exceptions import ConnectionError as AerospikeConnectionError

from aerospike_fluent.aio.client import FluentClient
from aerospike_fluent.dataset import DataSet
from aerospike_fluent.policy.behavior import Behavior

# Set up debug logging
logger = logging.getLogger(__name__)
# Enable debug output via environment variable or set to True directly
DEBUG_SYNC_CLIENT = os.environ.get("DEBUG_SYNC_CLIENT", "false").lower() == "true"
if DEBUG_SYNC_CLIENT:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if sys.version_info >= (3, 10):
    from typing import TypeGuard
else:
    from typing_extensions import TypeGuard

# Thread-local storage for event loop managers
_thread_local = threading.local()


class _EventLoopManager:
    """Manages an event loop for sync operations in a thread-safe way."""

    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._lock = threading.Lock()
        self._thread_id: Optional[int] = None
        self._refcount = 0  # Track how many clients are using this manager
    
    def __del__(self) -> None:
        """Ensure event loop is closed when manager is destroyed."""
        try:
            self.close()
        except Exception:
            pass
    
    def acquire(self) -> None:
        """Acquire a reference to this manager."""
        with self._lock:
            self._refcount += 1
    
    def release(self) -> None:
        """Release a reference to this manager. Closes the loop if no more references to free file descriptors."""
        loop_to_close = None
        with self._lock:
            self._refcount -= 1
            if self._refcount <= 0:
                # Close the loop when no more clients are using it to free file descriptors
                # This is critical to prevent "too many open files" errors
                if self._loop is not None:
                    loop_to_close = self._loop
                    self._loop = None
                    self._thread_id = None
                    if DEBUG_SYNC_CLIENT:
                        logger.debug(f"[release] Thread {threading.get_ident()}: Closing loop (refcount=0) to free file descriptors")
            else:
                if DEBUG_SYNC_CLIENT:
                    logger.debug(f"[release] Thread {threading.get_ident()}: Released reference (refcount={self._refcount})")
        
        # Close the loop outside the lock to avoid potential deadlocks
        if loop_to_close is not None:
            try:
                if not loop_to_close.is_closed():
                    if loop_to_close.is_running():
                        if DEBUG_SYNC_CLIENT:
                            logger.warning(f"[release] Thread {threading.get_ident()}: Loop is running, cannot close safely")
                    else:
                        try:
                            # Close the selector first (this releases file descriptors immediately)
                            try:
                                if hasattr(loop_to_close, '_selector') and loop_to_close._selector is not None:
                                    loop_to_close._selector.close()
                            except Exception:
                                pass
                            # Then close the loop
                            loop_to_close.close()
                            if loop_to_close.is_closed():
                                if DEBUG_SYNC_CLIENT:
                                    logger.debug(f"[release] Thread {threading.get_ident()}: Closed selector and loop successfully")
                            else:
                                if DEBUG_SYNC_CLIENT:
                                    logger.warning(f"[release] Thread {threading.get_ident()}: Loop close() did not close the loop")
                        except Exception as e:
                            if DEBUG_SYNC_CLIENT:
                                logger.warning(f"[release] Thread {threading.get_ident()}: Error closing loop: {e}")
                            try:
                                if hasattr(loop_to_close, '_closed'):
                                    loop_to_close._closed = True
                            except Exception:
                                pass
            except Exception as e:
                if DEBUG_SYNC_CLIENT:
                    logger.warning(f"[release] Thread {threading.get_ident()}: Exception during loop cleanup: {e}")
    
    def _reset_loop(self) -> None:
        """Reset the loop if it's in a bad state. Creates a new loop for the next use."""
        loop_to_close = None
        with self._lock:
            if self._loop is not None:
                loop_to_close = self._loop
                self._loop = None
                self._thread_id = None
        
        # Close the loop outside the lock to avoid potential deadlocks
        if loop_to_close is not None:
            try:
                if not loop_to_close.is_closed():
                    if loop_to_close.is_running():
                        if DEBUG_SYNC_CLIENT:
                            logger.warning(f"[_reset_loop] Thread {threading.get_ident()}: Loop is running, cannot close safely")
                    else:
                        try:
                            # Close the selector first (this releases file descriptors immediately)
                            try:
                                if hasattr(loop_to_close, '_selector') and loop_to_close._selector is not None:
                                    loop_to_close._selector.close()
                            except Exception:
                                pass
                            # Then close the loop
                            loop_to_close.close()
                            if loop_to_close.is_closed():
                                if DEBUG_SYNC_CLIENT:
                                    logger.debug(f"[_reset_loop] Thread {threading.get_ident()}: Closed selector and loop successfully")
                            else:
                                if DEBUG_SYNC_CLIENT:
                                    logger.warning(f"[_reset_loop] Thread {threading.get_ident()}: Loop close() did not close the loop")
                        except Exception as e:
                            if DEBUG_SYNC_CLIENT:
                                logger.warning(f"[_reset_loop] Thread {threading.get_ident()}: Error closing loop: {e}")
                            try:
                                if hasattr(loop_to_close, '_closed'):
                                    loop_to_close._closed = True
                            except Exception:
                                pass
            except Exception as e:
                if DEBUG_SYNC_CLIENT:
                    logger.warning(f"[_reset_loop] Thread {threading.get_ident()}: Exception during loop cleanup: {e}")

    def _get_or_create_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create an event loop for the current thread."""
        current_thread_id = threading.get_ident()
        if DEBUG_SYNC_CLIENT:
            logger.debug(f"[_get_or_create_loop] Thread {current_thread_id}: Starting, stored_thread_id={self._thread_id}, has_loop={self._loop is not None}")

        with self._lock:
            # If we're in the same thread and have a loop, reuse it
            if self._thread_id == current_thread_id and self._loop is not None:
                try:
                    # Check if loop is still valid and can be used
                    if self._loop.is_closed():
                        # Loop is closed, create a new one
                        if DEBUG_SYNC_CLIENT:
                            logger.warning(f"[_get_or_create_loop] Thread {current_thread_id}: Loop is closed, creating new one")
                        self._loop = None
                    else:
                        # Try to verify the loop is actually usable
                        # Check if it's running (can't use a running loop)
                        if self._loop.is_running():
                            if DEBUG_SYNC_CLIENT:
                                logger.warning(f"[_get_or_create_loop] Thread {current_thread_id}: Loop is running, creating new one")
                            self._loop = None
                        else:
                            # Try to verify the loop is actually usable
                            # by checking if we can get its default executor
                            try:
                                _ = self._loop._default_executor
                                if DEBUG_SYNC_CLIENT:
                                    logger.debug(f"[_get_or_create_loop] Thread {current_thread_id}: Reusing existing loop")
                                return self._loop
                            except (RuntimeError, AttributeError) as e:
                                # Loop is in a bad state, create a new one
                                if DEBUG_SYNC_CLIENT:
                                    logger.warning(f"[_get_or_create_loop] Thread {current_thread_id}: Loop validation failed: {e}, creating new one")
                                self._loop = None
                except (RuntimeError, AttributeError) as e:
                    # Loop is invalid, create a new one
                    if DEBUG_SYNC_CLIENT:
                        logger.warning(f"[_get_or_create_loop] Thread {current_thread_id}: Loop check exception: {e}, creating new one")
                    self._loop = None

            # Create a new loop
            if DEBUG_SYNC_CLIENT:
                logger.debug(f"[_get_or_create_loop] Thread {current_thread_id}: Creating new loop")
            self._loop = self._create_new_loop()
            self._thread_id = current_thread_id
            if DEBUG_SYNC_CLIENT:
                logger.debug(f"[_get_or_create_loop] Thread {current_thread_id}: Created new loop: {self._loop}, closed: {self._loop.is_closed()}")
            return self._loop

    def _create_new_loop(self) -> asyncio.AbstractEventLoop:
        """Create a new event loop."""
        if DEBUG_SYNC_CLIENT:
            logger.debug(f"[_create_new_loop] Thread {threading.get_ident()}: Starting")
        try:
            loop = asyncio.get_running_loop()
            # If we're in an async context, we can't use the sync client
            # The sync client manages its own loop and cannot coexist with a running loop
            if DEBUG_SYNC_CLIENT:
                logger.error(f"[_create_new_loop] Thread {threading.get_ident()}: Found running loop: {loop}")
            raise RuntimeError(
                "Cannot use SyncFluentClient from within an async context. "
                "Use FluentClient (async) instead, or ensure you're not in an async context."
            )
        except RuntimeError as e:
            # Check if this is our error or a "no running loop" error
            if "Cannot use SyncFluentClient" in str(e):
                raise
            # No running loop, create a new one
            # Don't use get_event_loop() as it may create a loop we don't manage
            if DEBUG_SYNC_CLIENT:
                logger.debug(f"[_create_new_loop] Thread {threading.get_ident()}: No running loop ({e}), creating new one")
            loop = asyncio.new_event_loop()
            # Don't set it as the default loop - we'll manage it ourselves
            if DEBUG_SYNC_CLIENT:
                logger.debug(f"[_create_new_loop] Thread {threading.get_ident()}: Created new loop: {loop}")
            return loop

    def run_async(self, coro) -> any:
        """Run an async coroutine synchronously."""
        if DEBUG_SYNC_CLIENT:
            logger.debug(f"[run_async] Thread {threading.get_ident()}: Starting")
        # Always use our managed loop for sync operations
        # This avoids complications with nest_asyncio and ensures consistent behavior
        # We create our own loop which is safe even if we're in an async context
        if DEBUG_SYNC_CLIENT:
            logger.debug(f"[run_async] Thread {threading.get_ident()}: Using managed loop")
        # Track if we're creating a new loop or reusing an existing one
        loop_was_reused = self._loop is not None and self._thread_id == threading.get_ident()
        loop = self._get_or_create_loop()
        loop_is_fresh = not loop_was_reused
        if DEBUG_SYNC_CLIENT:
            logger.debug(f"[run_async] Thread {threading.get_ident()}: Got loop: {loop}, closed: {loop.is_closed()}, fresh: {loop_is_fresh}")
        try:
            # Check if loop is running - if so, we can't use run_until_complete
            if loop.is_running():
                if DEBUG_SYNC_CLIENT:
                    logger.warning(f"[run_async] Thread {threading.get_ident()}: Loop is running, resetting")
                self._reset_loop()
                loop = self._get_or_create_loop()
                loop_is_fresh = True  # After reset, it's definitely fresh
                if DEBUG_SYNC_CLIENT:
                    logger.debug(f"[run_async] Thread {threading.get_ident()}: Got fresh loop after reset")
            
            # Ensure the loop is set as the current event loop for this thread
            # Some asyncio operations need the current loop to be set
            # Save the old loop if it exists (but don't create one if it doesn't)
            old_loop = None
            try:
                # Try to get the running loop first (won't create a new one)
                old_loop = asyncio.get_running_loop()
            except RuntimeError:
                # No running loop, check if there's a current event loop set
                # Use get_event_loop() carefully - in Python 3.10+ it might create a new loop
                try:
                    # Check if there's already a loop set for this thread
                    # We'll use a try/except to avoid creating one
                    if hasattr(asyncio, '_get_running_loop'):
                        # Python 3.7+ - try to get the thread-local loop without creating one
                        old_loop = None
                    else:
                        # Fallback - try get_event_loop but be careful
                        try:
                            old_loop = asyncio.get_event_loop()
                            # If it's closed or not our loop, ignore it
                            if old_loop.is_closed() or old_loop == loop:
                                old_loop = None
                        except RuntimeError:
                            old_loop = None
                except Exception:
                    old_loop = None
            
            # Always set our loop as the current event loop before using it
            # This is critical for asyncio operations to work correctly
            asyncio.set_event_loop(loop)
            
            if DEBUG_SYNC_CLIENT:
                logger.debug(f"[run_async] Thread {threading.get_ident()}: Set loop as current event loop")
            
            try:
                result = loop.run_until_complete(coro)
                if DEBUG_SYNC_CLIENT:
                    logger.debug(f"[run_async] Thread {threading.get_ident()}: Completed successfully (managed loop)")
                return result
            finally:
                # Restore the old loop if there was one and it's different from ours
                try:
                    current_loop = asyncio.get_event_loop()
                    if old_loop is not None and old_loop != current_loop and not old_loop.is_closed():
                        asyncio.set_event_loop(old_loop)
                    elif old_loop is None:
                        # No old loop to restore - keep our loop set for potential reuse
                        # Don't clear it unless we're resetting
                        pass
                except RuntimeError:
                    # Can't get/set event loop, that's okay
                    pass
        except RuntimeError as runtime_err:
            # RuntimeError from run_until_complete usually means the loop is in a bad state
            # Check if it's the "no running event loop" error or similar
            error_msg = str(runtime_err).lower()
            if "no running event loop" in error_msg or "this event loop is already running" in error_msg:
                if DEBUG_SYNC_CLIENT:
                    logger.warning(f"[run_async] Thread {threading.get_ident()}: Loop runtime error: {runtime_err}, resetting loop")
                self._reset_loop()
                # Try once more with a fresh loop
                loop = self._get_or_create_loop()
                if DEBUG_SYNC_CLIENT:
                    logger.debug(f"[run_async] Thread {threading.get_ident()}: Retrying with fresh loop")
                result = loop.run_until_complete(coro)
                if DEBUG_SYNC_CLIENT:
                    logger.debug(f"[run_async] Thread {threading.get_ident()}: Completed successfully after retry")
                return result
            # If it's a different RuntimeError, let it fall through to the general exception handler
            raise
        except StopAsyncIteration:
            # StopAsyncIteration is a normal part of async iteration protocol, not an error
            # Re-raise it without logging or resetting the loop
            raise
        except AerospikeConnectionError as e:
            # Connection errors can leave the event loop in a bad state, especially if we're reusing a loop
            # The loop might have pending operations, closed connections, or other state that prevents new connections
            # Always clear the loop reference to prevent reuse, but only close it if it was reused (has bad state)
            # Fresh loops that fail are likely due to external issues - closing them won't help and wastes resources
            if DEBUG_SYNC_CLIENT:
                logger.error(f"[run_async] Thread {threading.get_ident()}: ConnectionError occurred: {e}")
            if not loop_is_fresh:
                # Reset reused loops that fail - they might have bad state
                if DEBUG_SYNC_CLIENT:
                    logger.warning(f"[run_async] Thread {threading.get_ident()}: Resetting reused loop after ConnectionError to ensure clean state")
                self._reset_loop()
            else:
                # Fresh loop failed - likely external issue, just clear the reference
                # Don't close it immediately - let the loop manager handle cleanup when refcount reaches 0
                # This prevents unnecessary resource churn
                if DEBUG_SYNC_CLIENT:
                    logger.debug(f"[run_async] Thread {threading.get_ident()}: ConnectionError on fresh loop - clearing reference (likely external issue)")
            with self._lock:
                self._loop = None
                self._thread_id = None
            raise
        except Exception as e:
            # If an exception occurs, check if the loop is still valid
            # Only reset if the loop is actually closed or in a bad state
            if DEBUG_SYNC_CLIENT:
                logger.error(f"[run_async] Thread {threading.get_ident()}: Exception occurred: {type(e).__name__}: {e}")
            try:
                is_closed = loop.is_closed()
                if DEBUG_SYNC_CLIENT:
                    logger.debug(f"[run_async] Thread {threading.get_ident()}: Loop closed: {is_closed}")
                if is_closed:
                    # Loop is closed, clear it so a new one will be created
                    if DEBUG_SYNC_CLIENT:
                        logger.warning(f"[run_async] Thread {threading.get_ident()}: Loop is closed, clearing it")
                    with self._lock:
                        if self._loop is loop:
                            self._loop = None
                            self._thread_id = None
            except (RuntimeError, AttributeError) as loop_check_error:
                # Loop is in a bad state, clear it
                if DEBUG_SYNC_CLIENT:
                    logger.warning(f"[run_async] Thread {threading.get_ident()}: Loop check failed: {loop_check_error}, clearing loop")
                with self._lock:
                    if self._loop is loop:
                        self._loop = None
                        self._thread_id = None
            if DEBUG_SYNC_CLIENT:
                logger.error(f"[run_async] Thread {threading.get_ident()}: Re-raising exception: {type(e).__name__}: {e}")
            raise

    def close(self) -> None:
        """Close the event loop if we own it."""
        loop_to_close = None
        with self._lock:
            if self._loop is not None:
                loop_to_close = self._loop
                self._loop = None
                self._thread_id = None
        
        # Close the loop outside the lock to avoid potential deadlocks
        if loop_to_close is not None:
            try:
                if not loop_to_close.is_closed():
                    if loop_to_close.is_running():
                        if DEBUG_SYNC_CLIENT:
                            logger.warning(f"[close] Thread {threading.get_ident()}: Loop is running, cannot close safely")
                    else:
                        try:
                            # Close the selector first (this releases file descriptors immediately)
                            try:
                                if hasattr(loop_to_close, '_selector') and loop_to_close._selector is not None:
                                    loop_to_close._selector.close()
                            except Exception:
                                pass
                            # Then close the loop
                            loop_to_close.close()
                            if loop_to_close.is_closed():
                                if DEBUG_SYNC_CLIENT:
                                    logger.debug(f"[close] Thread {threading.get_ident()}: Closed selector and loop successfully")
                            else:
                                if DEBUG_SYNC_CLIENT:
                                    logger.warning(f"[close] Thread {threading.get_ident()}: Loop close() did not close the loop")
                        except Exception as e:
                            if DEBUG_SYNC_CLIENT:
                                logger.warning(f"[close] Thread {threading.get_ident()}: Error closing loop: {e}")
                            try:
                                if hasattr(loop_to_close, '_closed'):
                                    loop_to_close._closed = True
                            except Exception:
                                pass
            except Exception as e:
                if DEBUG_SYNC_CLIENT:
                    logger.warning(f"[close] Thread {threading.get_ident()}: Exception during loop cleanup: {e}")


class SyncFluentClient:
    """
    Synchronous wrapper for the FluentClient that hides async/await.

    This client provides the same fluent API as FluentClient but with
    synchronous methods, making it easier to use in non-async code.

    Example:
        ```python
        with SyncFluentClient("localhost:3000") as client:
            record = client.key_value(
                namespace="test",
                set_name="users",
                key="user123"
            ).get()
            print(record.bins if record else "Not found")
        ```

    Note:
        - This client wraps the async FluentClient internally
        - It uses asyncio event loops to execute async operations synchronously
        - If used from within an async context, you may need to install nest_asyncio
        - For better performance in async code, use FluentClient directly
    """

    def __init__(
        self,
        seeds: str,
        policy: Optional[ClientPolicy] = None,
    ) -> None:
        """
        Initialize a SyncFluentClient.

        Args:
            seeds: Aerospike cluster seed addresses (e.g., "localhost:3000")
            policy: Optional client policy. If None, a default policy is used.
        """
        self._seeds = seeds
        self._policy = policy
        self._async_client: Optional[FluentClient] = None
        # Reuse event loop manager per thread to avoid creating too many loops
        if not hasattr(_thread_local, 'loop_manager'):
            _thread_local.loop_manager = _EventLoopManager()
        self._loop_manager = _thread_local.loop_manager
        self._loop_manager.acquire()
        self._connected = False

    def connect(self) -> None:
        """Connect to the Aerospike cluster synchronously."""
        if DEBUG_SYNC_CLIENT:
            logger.debug(f"[connect] Thread {threading.get_ident()}: Starting, connected={self._connected}")
        if self._connected and self._async_client is not None:
            if DEBUG_SYNC_CLIENT:
                logger.debug(f"[connect] Thread {threading.get_ident()}: Already connected, returning")
            return

        async def _connect():
            if DEBUG_SYNC_CLIENT:
                logger.debug(f"[_connect] Thread {threading.get_ident()}: Creating FluentClient for {self._seeds}")
            client = FluentClient(self._seeds, self._policy)
            try:
                await client.connect()
                if DEBUG_SYNC_CLIENT:
                    logger.debug(f"[_connect] Thread {threading.get_ident()}: FluentClient connected")
                return client
            except Exception:
                # If connection fails, ensure the client is closed to prevent resource leaks
                try:
                    await client.close()
                except Exception:
                    pass
                raise

        try:
            if DEBUG_SYNC_CLIENT:
                logger.debug(f"[connect] Thread {threading.get_ident()}: Calling run_async to connect")
            self._async_client = self._loop_manager.run_async(_connect())
            self._connected = True
            if DEBUG_SYNC_CLIENT:
                logger.debug(f"[connect] Thread {threading.get_ident()}: Connection successful")
        except Exception as e:
            # If connection fails, ensure we're in a clean state
            self._connected = False
            if DEBUG_SYNC_CLIENT:
                logger.error(f"[connect] Thread {threading.get_ident()}: Connection failed: {type(e).__name__}: {e}")
            # Don't reset the loop on connection errors - they can happen for many reasons
            # The loop reset in run_async() will handle bad loop states
            raise

    def close(self) -> None:
        """Close the connection to the Aerospike cluster."""
        if self._async_client is not None:
            async def _close():
                await self._async_client.close()

            self._loop_manager.run_async(_close())
            self._async_client = None
            self._connected = False

        self._loop_manager.close()

    def __enter__(self) -> SyncFluentClient:
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[any],
    ) -> None:
        """Context manager exit."""
        try:
            self.close()
        except Exception:
            # Ensure we always release the reference, even if close fails
            if hasattr(self, '_loop_manager'):
                self._loop_manager.release()
            raise

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected."""
        return self._connected

    def _ensure_connected(self) -> FluentClient:
        """Ensure the client is connected and return the async client."""
        if not self._connected or self._async_client is None:
            self.connect()
        assert self._async_client is not None
        return self._async_client

    @overload
    def key_value(
        self,
        *,
        key: Key,
    ):
        """Create a key-value operation builder from a Key object."""
        ...

    @overload
    def key_value(
        self,
        *,
        dataset: DataSet,
        key: Union[str, int, bytes],
    ):
        """Create a key-value operation builder using a DataSet."""
        ...

    @overload
    def key_value(
        self,
        namespace: str,
        set_name: str,
        key: Union[str, int],
    ):
        """Create a key-value operation builder with explicit namespace/set."""
        ...

    def key_value(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        key: Optional[Union[str, int, bytes, Key]] = None,
        *,
        dataset: Optional[DataSet] = None,
    ):
        """
        Create a key-value operation builder (synchronous).

        Supports the same calling styles as FluentClient.key_value().
        Returns a wrapper that executes operations synchronously.
        """
        from aerospike_fluent.sync.operations.key_value import SyncKeyValueOperation

        # Delegate to async client to handle argument parsing
        async_client = self._ensure_connected()
        operation = async_client.key_value(
            namespace=namespace,
            set_name=set_name,
            key=key,
            dataset=dataset,
        )

        # Extract the namespace, set_name, and key from the operation
        return SyncKeyValueOperation(
            async_client=async_client,
            namespace=operation._namespace,
            set_name=operation._set_name,
            key=operation._key,
            loop_manager=self._loop_manager,
        )

    @overload
    def query(
        self,
        dataset: DataSet,
    ):
        """Create a query builder from a DataSet."""
        ...

    @overload
    def query(
        self,
        key: Key,
    ):
        """Create a query builder for a single Key (point read)."""
        ...

    @overload
    def query(
        self,
        keys: List[Key],
    ):
        """Create a query builder for multiple Keys (batch read)."""
        ...

    @overload
    def query(
        self,
        *keys: Key,
    ):
        """Create a query builder for multiple Keys (varargs)."""
        ...

    @overload
    def query(
        self,
        namespace: str,
        set_name: str,
    ):
        """Create a query builder with explicit namespace/set."""
        ...

    def query(
        self,
        arg1: Optional[Union[DataSet, Key, List[Key], str]] = None,
        arg2: Optional[str] = None,
        *keys: Key,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        dataset: Optional[DataSet] = None,
        key: Optional[Key] = None,
        keys_list: Optional[List[Key]] = None,
    ):
        """
        Create a query builder (synchronous).

        Supports the same calling styles as FluentClient.query().
        Returns a wrapper that executes queries synchronously.
        """
        from aerospike_fluent.sync.operations.query import SyncQueryBuilder

        # Delegate to async client - it now handles positional args
        async_client = self._ensure_connected()
        if arg1 is not None or arg2 is not None:
            # Has positional arguments - pass them positionally
            if arg2 is not None:
                # Two positional args (namespace, set_name)
                builder = async_client.query(arg1, arg2)
            else:
                # Single positional arg (DataSet, Key, or List[Key])
                builder = async_client.query(arg1)
        else:
            # Only keyword arguments
            builder = async_client.query(
                namespace=namespace,
                set_name=set_name,
                dataset=dataset,
                key=key,
                keys=keys_list,
            )

        return SyncQueryBuilder(
            async_client=async_client,
            namespace=builder._namespace,
            set_name=builder._set_name,
            loop_manager=self._loop_manager,
            query_builder=builder,  # Pass the builder to preserve single_key/keys
        )

    @overload
    def index(
        self,
        *,
        dataset: DataSet,
    ):
        """Create an index builder from a DataSet."""
        ...

    @overload
    def index(
        self,
        namespace: str,
        set_name: str,
    ):
        """Create an index builder with explicit namespace/set."""
        ...

    def index(
        self,
        namespace: Optional[str] = None,
        set_name: Optional[str] = None,
        *,
        dataset: Optional[DataSet] = None,
    ):
        """
        Create an index builder (synchronous).

        Supports the same calling styles as FluentClient.index().
        Returns a wrapper that executes index operations synchronously.
        """
        from aerospike_fluent.sync.operations.index import SyncIndexBuilder

        # Delegate to async client to handle argument parsing
        async_client = self._ensure_connected()
        builder = async_client.index(
            namespace=namespace,
            set_name=set_name,
            dataset=dataset,
        )

        return SyncIndexBuilder(
            async_client=async_client,
            namespace=builder._namespace,
            set_name=builder._set_name,
            loop_manager=self._loop_manager,
        )

    def truncate(self, dataset: DataSet, before_nanos: Optional[int] = None) -> None:
        """
        Truncate (delete all records) from a set (synchronous).

        This method deletes all records in the specified set.
        This operation cannot be undone.

        Args:
            dataset: The DataSet to truncate.
            before_nanos: Optional timestamp in nanoseconds. Only records with
                         last update time (LUT) less than this value will be
                         truncated. If None, all records in the set are truncated.

        Example:
            ```python
            users = DataSet.of("test", "users")
            client.truncate(users)

            # Truncate only records older than a specific time
            import time
            cutoff_time = time.time_ns() - (24 * 60 * 60 * 10**9)  # 24 hours ago
            client.truncate(users, before_nanos=cutoff_time)
            ```
        """
        async_client = self._ensure_connected()

        # Access the underlying async client and call its truncate method
        if async_client._client is None:
            raise RuntimeError("Client is not connected")

        async def _truncate():
            await async_client._client.truncate(
                dataset.namespace,
                dataset.set_name,
                before_nanos
            )

        self._loop_manager.run_async(_truncate())

    def create_session(self, behavior: Optional[Behavior] = None) -> "SyncSession":
        """
        Create a session with the specified behavior (synchronous).

        A session represents a logical connection to the cluster with specific
        behavior settings that control how operations are performed (timeouts,
        retry policies, consistency levels, etc.).

        Args:
            behavior: The behavior configuration for the session.
                     If None, uses Behavior.DEFAULT.

        Returns:
            A new SyncSession instance.

        Example:
            ```python
            # Create a session with default behavior
            session = client.create_session()

            # Create a session with custom behavior
            from datetime import timedelta
            fast_behavior = Behavior.DEFAULT.derive_with_changes(
                name="fast",
                total_timeout=timedelta(seconds=5)
            )
            session = client.create_session(fast_behavior)
            ```
        """
        from aerospike_fluent.sync.session import SyncSession

        async_client = self._ensure_connected()
        async_session = async_client.create_session(behavior)
        return SyncSession(async_session, self._loop_manager)

