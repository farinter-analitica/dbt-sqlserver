import smbclient

smbclient.register_session(
    server="10.0.4.157",
    username="FARINTERNET\ANALITICA",
    password="Cuatro4-Dos2=2",
    #encrypt=True,
)

print(
    smbclient.listdir(
        "\\10.0.4.157\data_repo\grupo_farinter\presupuesto_ventas_finanzas"
    )
)
