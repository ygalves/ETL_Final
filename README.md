# ETL_FInal

# Guía de Instalación: VS Code, pyenv, PostgreSQL, Airflow y Metabase

## Introducción
Este procedimiento asegura la integración entre VS Code, pyenv, PostgreSQL, Airflow y Metabase para ejecutar un laboratorio de ETL. Durante la implementación, se identificaron algunos problemas conocidos:

- Los DAGs independientes funcionan óptimamente.
- En el laboratorio, los DAGs se iniciaron manualmente. Al ser una instalación local, Airflow no permitió la serialización de DAGs.
- El DAG maestro no funciona debido a que no se logró normalizar la forma de leer los archivos YAML para los diferentes DAGs dentro del tiempo disponible.
- La conexión del DAG maestro con Metabase para actualizar automáticamente los dashboards no fue completada.
- SQLAlchemy ha tenido cambios recientes que afectan cómo Pandas utiliza el motor de base de datos. Estos DAGs han solucionado este problema.

---

## Selecciona tu sistema operativo:

- [Instalación en Linux](#instalación-en-linux)
- [Instalación en Windows](#instalación-en-windows)
- [Instalación en macOS](#instalación-en-macos)

---

## Instalación en Linux

[🔼 Volver al inicio](#guía-de-instalación-vs-code-pyenv-postgresql-airflow-y-metabase)

### Instalación de VS Code
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install wget gpg -y
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | sudo tee /usr/share/keyrings/packages.microsoft.gpg > /dev/null
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/packages.microsoft.gpg] https://packages.microsoft.com/repos/code stable main" | sudo tee /etc/apt/sources.list.d/vscode.list
sudo apt update
sudo apt install code -y
code --version
```

### Creación de Área de Trabajo
Toda la instalación debe realizarse dentro de una carpeta de trabajo. Primero, creamos la estructura de directorios:
```bash
mkdir -p ~/workspace/{dags/{credentials,Data,sql},metabase,logs}
```
En la carpeta `dags/credentials`, se deben crear tres archivos YAML:
```bash
touch ~/workspace/dags/credentials/config.yaml \
      ~/workspace/dags/credentials/credentials.yaml \
      ~/workspace/dags/credentials/mb_credentials.yaml
```

## Configuración de Airflow para detectar los DAGs
Edita el archivo `airflow.cfg` para que Airflow encuentre los DAGs en la carpeta correcta:
```bash
nano ~/airflow/airflow.cfg
```
Modifica la línea:
```ini
dags_folder = ~/workspace/dags
```
Guarda y reinicia Airflow:
```bash
airflow scheduler &
airflow webserver &
```

## Configuración de PostgreSQL para permitir conexiones externas

[🔼 Volver al inicio](#guía-de-instalación-vs-code-pyenv-postgresql-airflow-y-metabase)

Edita `postgresql.conf` para permitir conexiones remotas:
```bash
sudo nano /etc/postgresql/14/main/postgresql.conf
```
Modifica:
```ini
listen_addresses = '*'
```
Edita `pg_hba.conf` para aceptar conexiones externas:
```bash
sudo nano /etc/postgresql/14/main/pg_hba.conf
```
Agrega al final:
```ini
host    all             all             0.0.0.0/0               md5
```
Reinicia PostgreSQL:
```bash
sudo systemctl restart postgresql
```
Verifica que PostgreSQL está escuchando en el puerto 5432:
```bash
sudo netstat -tulnp | grep postgres
```

## Instalación y Configuración de pyenv
### Instalación de Dependencias
```bash
sudo apt update
sudo apt install -y make build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
    libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
```

### Instalación de pyenv
```bash
git clone https://github.com/pyenv/pyenv.git ~/.pyenv
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init --path)"' >> ~/.bashrc
source ~/.bashrc
```

### Instalación de Python con pyenv
```bash
pyenv install 3.11.6
pyenv global 3.11.6
python --version
```

### Importancia de las Versiones Correctas en Airflow

[🔼 Volver al inicio](#guía-de-instalación-vs-code-pyenv-postgresql-airflow-y-metabase)

Apache Airflow depende de versiones específicas de Python y bibliotecas como `SQLAlchemy`, `psycopg2-binary` y `Celery`. Para evitar problemas, usa los archivos de restricciones oficiales:
```bash
pip install --upgrade pip setuptools wheel
pip install apache-airflow[postgres,celery] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
```
Se recomienda usar `psycopg2-binary` en vez de `psycopg2`.

### Creación de Entorno Virtual y Dependencias
#### Usando pip
```bash
cd ~/workspace
pyenv virtualenv 3.11.6 airflow-env
pyenv activate airflow-env
pip install --upgrade pip setuptools wheel
pip install apache-airflow[postgres,celery] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
pip install psycopg2-binary SQLAlchemy
```
#### Usando Poetry
```bash
cd ~/workspace
pyenv virtualenv 3.11.6 airflow-env
pyenv activate airflow-env
poetry init
poetry add apache-airflow[postgres,celery] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
poetry add psycopg2-binary SQLAlchemy
```

## Instalación con Docker
### PostgreSQL
```bash
docker run --name postgres_container -e POSTGRES_USER=miusuario -e POSTGRES_PASSWORD=mipassword -e POSTGRES_DB=midb -p 5432:5432 -d postgres
```
### Airflow
```bash
git clone https://github.com/apache/airflow.git
cd airflow
docker-compose up
```
### Metabase
```bash
docker run --name metabase_container -p 3000:3000 -d metabase/metabase
```

---

## Instalación en Windows

[🔼 Volver al inicio](#guía-de-instalación-vs-code-pyenv-postgresql-airflow-y-metabase)

### Instalación de VS Code
1. Descarga e instala VS Code desde [aquí](https://code.visualstudio.com/Download).
2. Durante la instalación, selecciona la opción "Agregar al PATH".
3. Verifica la instalación con:
   ```powershell
   code --version
   ```

### Creación de Área de Trabajo
Toda la instalación debe realizarse dentro de una carpeta de trabajo. Crear la estructura de directorios:
```powershell
mkdir -p C:\workspace\dags\{credentials,Data,sql}
mkdir C:\workspace\metabase
mkdir C:\workspace\logs
```
Crear los archivos YAML en `credentials`:
```powershell
New-Item C:\workspace\dags\credentials\config.yaml -ItemType File
New-Item C:\workspace\dags\credentials\credentials.yaml -ItemType File
New-Item C:\workspace\dags\credentials\mb_credentials.yaml -ItemType File
```

## Instalación de PostgreSQL
Descargar e instalar desde [PostgreSQL](https://www.postgresql.org/download/windows/).

Agregar PostgreSQL al PATH:
```powershell
$env:Path += ";C:\Program Files\PostgreSQL\14\bin"
```
Verificar la instalación:
```powershell
psql --version
```

### Configuración para Conexiones Externas
Editar `postgresql.conf`:
```ini
listen_addresses = '*'
```
Editar `pg_hba.conf`:
```ini
host    all             all             0.0.0.0/0               md5
```
Reiniciar PostgreSQL:
```powershell
net stop postgresql
net start postgresql
```

## Instalación y Configuración de pyenv
Instalar [pyenv-win](https://github.com/pyenv-win/pyenv-win) siguiendo su documentación oficial.

Agregar pyenv al PATH:
```powershell
$env:Path += ";$HOME\.pyenv\pyenv-win\bin;$HOME\.pyenv\pyenv-win\shims"
```

Instalar Python con pyenv:
```powershell
pyenv install 3.11.6
pyenv global 3.11.6
python --version
```

## Instalación de Airflow

[🔼 Volver al inicio](#guía-de-instalación-vs-code-pyenv-postgresql-airflow-y-metabase)

Actualizar `pip`:
```powershell
python -m pip install --upgrade pip setuptools wheel
```

Instalar Airflow con restricciones:
```powershell
pip install apache-airflow[postgres,celery] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
```

Se recomienda instalar `psycopg2-binary` en lugar de `psycopg2`:
```powershell
pip install psycopg2-binary SQLAlchemy
```

### Creación de Entorno Virtual y Dependencias
#### Usando pip
```powershell
cd C:\workspace
pyenv virtualenv 3.11.6 airflow-env
pyenv activate airflow-env
pip install --upgrade pip setuptools wheel
pip install apache-airflow[postgres,celery] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
pip install psycopg2-binary SQLAlchemy
```
#### Usando Poetry
```powershell
cd C:\workspace
pyenv virtualenv 3.11.6 airflow-env
pyenv activate airflow-env
poetry init
poetry add apache-airflow[postgres,celery] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
poetry add psycopg2-binary SQLAlchemy
```

## Instalación con Docker
### PostgreSQL
```powershell
docker run --name postgres_container -e POSTGRES_USER=miusuario -e POSTGRES_PASSWORD=mipassword -e POSTGRES_DB=midb -p 5432:5432 -d postgres
```
### Airflow
```powershell
git clone https://github.com/apache/airflow.git
cd airflow
docker-compose up
```
### Metabase
```powershell
docker run --name metabase_container -p 3000:3000 -d metabase/metabase
```

---

## Instalación en macOS

[🔼 Volver al inicio](#guía-de-instalación-vs-code-pyenv-postgresql-airflow-y-metabase)

### Instalación de VS Code
```bash
brew install --cask visual-studio-code
code --version
```

### Creación de Área de Trabajo
Toda la instalación debe realizarse dentro de una carpeta de trabajo. Primero, creamos la estructura de directorios:
```bash
mkdir -p ~/workspace/{dags/{credentials,Data,sql},metabase,logs}
```
En la carpeta `dags/credentials`, se deben crear tres archivos YAML:
```bash
touch ~/workspace/dags/credentials/config.yaml \
      ~/workspace/dags/credentials/credentials.yaml \
      ~/workspace/dags/credentials/mb_credentials.yaml
```

## Configuración de Airflow para detectar los DAGs
Edita el archivo `airflow.cfg` para que Airflow encuentre los DAGs en la carpeta correcta:
```bash
nano ~/airflow/airflow.cfg
```
Modifica la línea:
```ini
dags_folder = ~/workspace/dags
```
Guarda y reinicia Airflow:
```bash
airflow scheduler &
airflow webserver &
```

## Instalación de PostgreSQL
```bash
brew install postgresql
brew services start postgresql
```

Verifica que PostgreSQL esté corriendo:
```bash
pg_ctl -D /usr/local/var/postgres status
```

## Instalación y Configuración de pyenv

[🔼 Volver al inicio](#guía-de-instalación-vs-code-pyenv-postgresql-airflow-y-metabase)

```bash
brew install pyenv
```
Agrega lo siguiente al archivo `~/.zshrc` o `~/.bashrc`:
```bash
echo 'eval "$(pyenv init --path)"' >> ~/.zshrc
source ~/.zshrc
```

### Instalación de Python con pyenv
```bash
pyenv install 3.11.6
pyenv global 3.11.6
python --version
```

### Importancia de las Versiones Correctas en Airflow
Apache Airflow depende de versiones específicas de Python y bibliotecas como `SQLAlchemy`, `psycopg2-binary` y `Celery`. Para evitar problemas, usa los archivos de restricciones oficiales:
```bash
pip install --upgrade pip setuptools wheel
pip install apache-airflow[postgres,celery] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
```
Se recomienda usar `psycopg2-binary` en vez de `psycopg2`.

### Creación de Entorno Virtual y Dependencias
#### Usando pip
```bash
cd ~/workspace
pyenv virtualenv 3.11.6 airflow-env
pyenv activate airflow-env
pip install --upgrade pip setuptools wheel
pip install apache-airflow[postgres,celery] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
pip install psycopg2-binary SQLAlchemy
```
#### Usando Poetry

[🔼 Volver al inicio](#guía-de-instalación-vs-code-pyenv-postgresql-airflow-y-metabase)

```bash
cd ~/workspace
pyenv virtualenv 3.11.6 airflow-env
pyenv activate airflow-env
poetry init
poetry add apache-airflow[postgres,celery] --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.11.txt"
poetry add psycopg2-binary SQLAlchemy
```

## Instalación con Docker
### PostgreSQL
```bash
docker run --name postgres_container -e POSTGRES_USER=miusuario -e POSTGRES_PASSWORD=mipassword -e POSTGRES_DB=midb -p 5432:5432 -d postgres
```
### Airflow
```bash
git clone https://github.com/apache/airflow.git
cd airflow
docker-compose up
```
### Metabase
```bash
docker run --name metabase_container -p 3000:3000 -d metabase/metabase
```
---

## Recursos Adicionales
- [Documentación de PostgreSQL](https://www.postgresql.org/docs/)
- [Documentación de Apache Airflow](https://airflow.apache.org/docs/)
- [Documentación de Metabase](https://www.metabase.com/docs/)

---

## Nota sobre psycopg2 vs psycopg2-binary
Se recomienda usar `psycopg2-binary` en lugar de `psycopg2` por las siguientes razones:

1. **Fácil instalación**: `psycopg2-binary` incluye las bibliotecas necesarias precompiladas, evitando problemas de compilación.
2. **Compatibilidad inmediata**: Funciona en la mayoría de los sistemas sin configuración adicional.
3. **Recomendación oficial**: Los desarrolladores sugieren `psycopg2-binary` para desarrollo, pero `psycopg2` para producción por estabilidad. Ver [documentación oficial](https://www.psycopg.org/docs/install.html#binary-installation).
4. **Posibles problemas en producción**: `psycopg2-binary` usa una versión precompilada de `libpq`, lo que podría causar problemas de compatibilidad en ciertos entornos.

**Recomendación:** Para desarrollo y pruebas, usa `psycopg2-binary`. Para producción, considera `psycopg2` con configuración específica.

[🔼 Volver al inicio](#guía-de-instalación-vs-code-pyenv-postgresql-airflow-y-metabase)
---
>>>>>>> daed0edd (Initial commit)
