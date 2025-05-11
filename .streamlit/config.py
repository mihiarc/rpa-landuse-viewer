"""
Streamlit Configuration Settings
This file overrides default Streamlit settings for deployment.
"""
import streamlit as st

# Force specific Python version and pip instead of uv
st.set_option('server.deploymentTarget', 'pip')

# Configure server settings
st.set_option('server.enableXsrfProtection', True)
st.set_option('server.enableCORS', False)

# Additional theme settings
st.set_option('theme.primaryColor', '#3d7b6c')
st.set_option('theme.backgroundColor', '#f5f5f5')
st.set_option('theme.secondaryBackgroundColor', '#e6f7f2')
st.set_option('theme.textColor', '#262730')
st.set_option('theme.font', 'sans serif') 