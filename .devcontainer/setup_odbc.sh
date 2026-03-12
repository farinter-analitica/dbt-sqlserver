sudo install -d -m 0755 /usr/share/keyrings
curl -fsSL https://packages.microsoft.com/keys/microsoft-rolling.asc | gpg --dearmor | sudo tee /usr/share/keyrings/microsoft-prod.gpg >/dev/null

#Download appropriate package for the OS version
#Choose only ONE of the following, corresponding to your OS version

#Debian 13 (trixie)
curl -fsSL https://packages.microsoft.com/config/debian/13/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list >/dev/null

sudo apt update
sudo ACCEPT_EULA=Y apt install -y msodbcsql18
# optional: for bcp and sqlcmd
sudo ACCEPT_EULA=Y apt install -y mssql-tools18
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc
# optional: for unixODBC development headers
sudo apt install -y unixodbc-dev
# optional: kerberos library for debian-slim distributions
sudo apt install -y libgssapi-krb5-2
