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

"""TlsBuilder - Builder for configuring TLS settings."""

from __future__ import annotations

import typing
from typing import Optional, Any

if typing.TYPE_CHECKING:
    from aerospike_fluent.aio.cluster_definition import ClusterDefinition


class TlsBuilder:
    """
    Builder class for configuring TLS settings for Aerospike cluster connections.
    
    This class provides a fluent API for configuring TLS parameters such as
    TLS name, CA file, protocols, ciphers, and other TLS-specific options.
    
    Simple example usage:
        ```python
        cluster = ClusterDefinition("localhost", 3100)\
            .with_tls_config_of()\
                .tls_name("myTlsName")\
                .ca_file("myCaFile")\
            .done()\
            .with_native_credentials("myUser", "password")
        ```
    """
    
    def __init__(self, parent: ClusterDefinition) -> None:
        """
        Initialize a TlsBuilder.
        
        Args:
            parent: The parent ClusterDefinition
        """
        self._parent = parent
        self._tls_name: Optional[str] = None
        self._ca_file: Optional[str] = None
        self._protocols: Optional[list[str]] = None
        self._ciphers: Optional[list[str]] = None
        self._for_login_only = False
        self._tls_enabled = True
    
    def tls_name(self, tls_name: str) -> TlsBuilder:
        """
        Sets the TLS name for server certificate validation and hostname verification.
        
        This TLS name will be applied to all Host objects that don't already have
        a TLS name set. The TLS name is used for:
        - Certificate validation: Verifies the server certificate matches this name
        - SNI (Server Name Indication): Tells the server which certificate to present
        - Hostname override: Allows validation against a different name than the connection address
        
        Args:
            tls_name: The TLS name for certificate validation and hostname verification
        
        Returns:
            This TlsBuilder for method chaining
        """
        self._tls_name = tls_name
        return self
    
    def ca_file(self, ca_file: str) -> TlsBuilder:
        """
        Sets the path to the Certificate Authority (CA) PEM file.
        
        The CA file contains the certificates used to verify the server's identity.
        This method supports PEM-formatted certificate files for easy certificate management.
        
        Args:
            ca_file: The path to the CA certificate PEM file
        
        Returns:
            This TlsBuilder for method chaining
        """
        self._ca_file = ca_file
        return self
    
    def protocols(self, *protocols: str) -> TlsBuilder:
        """
        Sets the allowed TLS protocols.
        
        Args:
            *protocols: The TLS protocols to allow (e.g., "TLSv1.2", "TLSv1.3")
        
        Returns:
            This TlsBuilder for method chaining
        """
        self._protocols = list(protocols) if protocols else None
        return self
    
    def ciphers(self, *ciphers: str) -> TlsBuilder:
        """
        Sets the allowed TLS cipher suites.
        
        Args:
            *ciphers: The cipher suite names to allow
        
        Returns:
            This TlsBuilder for method chaining
        """
        self._ciphers = list(ciphers) if ciphers else None
        return self
    
    def for_login_only(self, for_login_only: bool = True) -> TlsBuilder:
        """
        Sets whether TLS should be used only for login/authentication.
        
        Args:
            for_login_only: If True, TLS is only used for authentication
        
        Returns:
            This TlsBuilder for method chaining
        """
        self._for_login_only = for_login_only
        return self
    
    def done(self) -> ClusterDefinition:
        """
        Completes TLS configuration and returns to the parent ClusterDefinition.
        
        Returns:
            The parent ClusterDefinition for continued method chaining
        """
        return self._parent
    
    def is_tls_enabled(self) -> bool:
        """Check if TLS is enabled."""
        return self._tls_enabled
    
    def get_tls_name(self) -> Optional[str]:
        """Get the TLS name."""
        return self._tls_name
    
    def build_tls_policy(self) -> Any:
        """
        Build a TLS policy from the configuration.
        
        Returns:
            A configured TLS policy object (type depends on client implementation)
        
        Note:
            TLS policy structure may vary by client implementation.
            This is a placeholder that returns a dict for now.
        """
        # Note: Python Async Client TLS support may be limited
        # This returns a dict that can be used to configure TLS when support is added
        policy: dict[str, Any] = {}
        
        if self._ca_file:
            policy["ca_file"] = self._ca_file
        
        if self._protocols:
            policy["protocols"] = self._protocols
        
        if self._ciphers:
            policy["ciphers"] = self._ciphers
        
        policy["for_login_only"] = self._for_login_only
        
        return policy

