## Despliegue de barometro de bienestar
> Para desplegar el backed del sistema, se debe tener instalado docker con las imagenes de "Postgress" y "pgadmin" 

En consola dentro de la carpeta raiz se debe iniciar con 
``` bash
    docker-compose up --build
```
Tambien se puede iniciar con
``` bash
    docker-compose up -d
```

## Visualización de base de datos mediante PGADMIN4
Se debe ingresar al navegador mediante el url 
``` web
    localhost: **3380** {puertos pgadmin}
```
Una vez ingresado, se pediran las credenciales, en este caso siendo.
> Correo: **Admin@admin**
> Passwoord: **admin**

## Vinculación PGADMIN DB
Para vincular la base de datos en PGADMIN se debe hacer click derecho en servidor, para luego presionar register server.
> **General**
> Name: { El de su preferencia }
> **Connection**
> Host_name: **databases** { O el nombre que tenga el contenedor de la base de datos a ingresar }x
> Port: **5542** { Puerto de la base de datos }






