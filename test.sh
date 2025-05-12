#!/usr/bin/env bash
set -u  # error si variable non définie

# Vérification du paramètre
if (( $# < 1 )); then
  echo "Usage: $0 <executable> [args...]"
  exit 1
fi

# Récupération de l’exécutable (avec ses éventuels arguments)
exe="$1"; shift

max=100
count=0

while (( count < max )); do
  echo "Itération $((count+1)) : exécutionde '$exe $*'..."
  $exe $@
  status=$?
  if (( status != 0 )); then
    echo "Erreur détectée (code : $status). Arrêt de la boucle."
    break  # quitte la boucle dès qu'une erreur survient :contentReference[oaicite:0]{index=0}
  fi
  (( count++ ))
done

if (( count == max )); then
  echo "Limite de $max itérations atteinte sans erreur."
else
  echo "Boucle arrêtée après $((count+1)) itérations en raison d'une erreur."
fi
