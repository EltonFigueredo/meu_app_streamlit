import streamlit as st
import pandas as pd
from mpp import read as mpp_read
    
uploaded_file = st.file_uploader("Escolha um ficheiro .mpp", type="mpp")

if uploaded_file is not None:
    # Use o ficheiro enviado para ler o conteúdo do .mpp
    mpp_data = mpp_read(uploaded_file)
    df = pd.DataFrame(mpp_data)

if uploaded_file is not None:
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Descarregar .xlsx",
        data=csv,
        file_name='ficheiro_convertido.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )