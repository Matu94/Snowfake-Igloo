import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from cryptography.hazmat.primitives import serialization

def get_session():
    """
    Robust connection handler:
    1. Checks for SiS (Active Session).
    2. Checks for Key Pair Auth (Local).
    3. Checks for Password/Browser Auth (Local Fallback).
    """
    # 1. Try Active Session (Running in Snowflake)
    try:
        return get_active_session()
    except Exception:
        pass

    # 2. Local Connection Logic
    if "snowflake" in st.secrets:
        config = st.secrets["snowflake"].to_dict()
        
        # A. Handle Key Pair Auth (The "Senior" Way)
        if "private_key_path" in config:
            try:
                # Read the private key file
                with open(config["private_key_path"], "rb") as key_file:
                    p_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=None
                    )
                
                # Snowpark expects the raw bytes of the key
                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                
                config["private_key"] = pkb
                del config["private_key_path"] # Clean up param not needed by Snowpark
                
                return Session.builder.configs(config).create()
                
            except Exception as e:
                st.error(f"Key Pair Login failed: {e}")
                return None

        # B. Handle Standard Auth (Password/ExternalBrowser)
        else:
            try:
                return Session.builder.configs(config).create()
            except Exception as e:
                st.error(f"Standard Login failed: {e}")
                return None
    
    st.error("No active session and no secrets found.")
    return None