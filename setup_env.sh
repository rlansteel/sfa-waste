#!/bin/bash

set -e  # Salir al primer error

echo "🔧 Verificando que pyenv esté correctamente configurado..."

if ! command -v pyenv &> /dev/null; then
  echo "❌ pyenv no está disponible en esta terminal."
  echo "👉 Solución recomendada:"
  echo ""
  echo "  export PYENV_ROOT=\"\$HOME/.pyenv\""
  echo "  export PATH=\"\$PYENV_ROOT/bin:\$PATH\""
  echo "  eval \"\$(pyenv init --path)\""
  echo "  eval \"\$(pyenv init -)\""
  exit 1
fi

# Asegura que pyenv esté bien cargado
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"

echo "🔍 Verificando si Python 3.10.12 está instalado..."
if ! pyenv versions --bare | grep -q "^3.10.12$"; then
  echo "📥 Instalando Python 3.10.12..."
  pyenv install 3.10.12
else
  echo "✅ Python 3.10.12 ya está instalado."
fi

echo "📌 Estableciendo Python 3.10.12 como versión local para este proyecto..."
pyenv local 3.10.12

PYTHON_PATH="$(pyenv which python)"
echo "🧪 Usando $PYTHON_PATH para entorno virtual"

# Verifica si .venv existe y si es válido
if [ -d ".venv" ]; then
  echo "🔎 Detectado entorno virtual .venv. Verificando versión..."
  if ! .venv/bin/python --version 2>/dev/null | grep -q "3.10.12"; then
    echo "❌ .venv no usa Python 3.10.12. Eliminando..."
    rm -rf .venv
    echo "🔁 Recreando entorno virtual con Python 3.10.12..."
    $PYTHON_PATH -m venv .venv
  else
    echo "✅ .venv ya está correctamente configurado."
  fi
else
  echo "🐍 Creando entorno virtual con Python 3.10.12..."
  $PYTHON_PATH -m venv .venv
fi

echo "✅ Activando entorno virtual..."
source .venv/bin/activate

echo "⬆️ Actualizando pip..."
pip install --upgrade pip

echo "📦 Instalando dependencias desde requirements.txt..."
pip install -r requirements.txt

echo ""
echo "📋 RESUMEN DEL ENTORNO"
echo "-------------------------"
echo "🧠 Python activo: $(which python)"
echo "🐍 Versión Python: $(python --version)"
echo "📚 Paquetes instalados:"
pip list
echo ""
echo "✅ Configuración finalizada."
echo "🔁 Para reactivar el entorno: source .venv/bin/activate"
