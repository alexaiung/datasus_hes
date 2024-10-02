# %%
import pandas as pd
from ftplib import FTP
from pysus.ftp.databases.sih import SIH

# %%
sih = SIH().load()

#%%
sih
sih.metadata
sih.groups

# %%
print(str(len(sih.get_files([group for group in sih.groups]))) + ' files')
print([group for group in sih.groups])
print([x for x in range(1, 13)])
for group in sih.groups:
    print(group, len(sih.get_files(group)))

# %%
files = sih.get_files('RD', uf='SP', year=2024, month=[x for x in range (1, 13)])

# %%
sih.describe(files[-1])
parquet = sih.download(files)

# %%
df = parquet[0].to_dataframe()
df.info()

# %%
df = df[df['CGC_HOSP'] == '46374500013768'] 
df.info()
print(len(df.columns))
for col in df.columns:
    print(col)
paciente_0 = df.iloc[0, :]

# %%
df['DIAG_PRINC'].value_counts()

# %%
cid_nome = pd.read_csv('data/CID-10-SUBCATEGORIAS.CSV', sep=';', encoding='ISO-8859-1')
cid_nome.head()

# %%
cid_nome[cid_nome['SUBCAT'] == 'O800']
#%%

df['DIAG_PRINC'].value_counts().reset_index().merge(cid_nome, left_on='DIAG_PRINC', right_on='SUBCAT')[['DIAG_PRINC', 'DESCRICAO', 'count']]
# MÉDIA DE IDADE DE TEMPO DE INTERNAÇÃO SEILA