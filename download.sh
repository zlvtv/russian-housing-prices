#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
data="${1:-$root/../datasets/russia_real_estate}"
mkdir -p "$data"

fetch() {
  local name="$1" url="$2" bytes="$3" sha="${4:-}"
  local path="$data/$name" temp="$data/$name.part"
  if [[ -f "$path" && "$(wc -c < "$path" | tr -d ' ')" == "$bytes" ]]; then
    if [[ -z "$sha" ]] || printf '%s  %s\n' "$sha" "$path" | shasum -a 256 -c -; then
      printf 'Found %s\n' "$name"
      return
    fi
  fi
  curl --fail --location --retry 2 --connect-timeout 20 "$url" --output "$temp"
  if [[ "$(wc -c < "$temp" | tr -d ' ')" != "$bytes" ]]; then
    printf 'Unexpected download length for %s\n' "$name" >&2
    return 1
  fi
  if [[ -n "$sha" ]]; then
    printf '%s  %s\n' "$sha" "$temp" | shasum -a 256 -c -
  fi
  mv "$temp" "$path"
}

fetch real_property_deals_russia_2024-2026.csv \
  'https://huggingface.co/datasets/Cyrill-Murashev/real-property-transactions-russia-2024-2026/resolve/f9383aa/real_property_deals_russia_2024-2026.csv?download=true' \
  1330536491 7aa75992bd7e4cd329cf8fda8faa8d73606be92f5b4a477dee1a66765975540c
fetch gisgkh_20260917.csv \
  'https://storage.yandexcloud.net/tochno-st-catalog/Minstroy/data_gisgkh_157_v20260917/data_gisgkh_157_v20260917.csv' \
  584316632 f0188764e4c9030c7321a6afbed869fcc6eb411fd2d9d8b83cd44f07904e872d
fetch cian_2024_2025.csv \
  'https://huggingface.co/datasets/cianml/ru-real-estate-geo-temporal-clusters/resolve/7af2ce5/russia_real_estate.csv?download=true' \
  278191340 39f9b405f9ed8749f1c63587d67cc7ebcc91b01845be7b485777a11bebe16a86
