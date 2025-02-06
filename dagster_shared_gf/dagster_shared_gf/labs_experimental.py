import pendulum as pdl

meses_muestra = 2
lista_fechas_muestra = [
    pdl.today().subtract(months=i+1) for i in range(0, meses_muestra)
]
lista_aniomes = [fecha.year * 100 + fecha.month for fecha in lista_fechas_muestra]

print(lista_aniomes)
