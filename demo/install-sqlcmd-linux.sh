#!/bin/bash
# Install Microsoft SQL Server command-line tools on Linux (Ubuntu/Debian)

set -e

echo "Installing SQL Server command-line tools for Linux..."
echo ""

# Import Microsoft GPG key
if ! [ -f /usr/share/keyrings/microsoft.gpg ]; then
    echo "Adding Microsoft repository key..."
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
fi

# Add Microsoft repository
if ! [ -f /etc/apt/sources.list.d/mssql-release.list ]; then
    echo "Adding Microsoft repository..."
    curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | \
        sudo tee /etc/apt/sources.list.d/mssql-release.list
fi

# Update package list
echo "Updating package list..."
sudo apt-get update

# Install sqlcmd and bcp
echo "Installing mssql-tools18..."
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev

# Add to PATH
if ! grep -q "/opt/mssql-tools18/bin" ~/.bashrc; then
    echo "Adding sqlcmd to PATH..."
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
    export PATH="$PATH:/opt/mssql-tools18/bin"
fi

echo ""
echo "✓ Installation complete!"
echo ""
echo "To use sqlcmd, either:"
echo "  1. Restart your terminal, or"
echo "  2. Run: source ~/.bashrc"
echo ""
echo "Then verify with: sqlcmd -?"
echo ""
echo "To connect to a SQL Server:"
echo "  sqlcmd -S server_name -U username -P password"
echo ""
