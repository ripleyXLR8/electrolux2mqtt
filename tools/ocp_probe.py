#!/usr/bin/env python3
"""Sonde l'API interne OCP et la compare à l'API officielle. EXPÉRIMENTAL.

Cet outil ne fait **rien d'autre que lire**. Il répond à une seule question :
l'API interne d'Electrolux — celle de l'application mobile — expose-t-elle
quelque chose que l'API officielle du portail développeur n'expose pas ?

Tant que la réponse est « non », il n'y a aucune raison d'écrire une passerelle
OCP : elle coûterait le rejeu du client d'authentification de l'application et
le stockage d'un jeton de compte, pour une donnée qu'on a déjà.

Pourquoi cet outil existe malgré tout : la question ne se tranche pas sur le
papier. Les jetons du portail développeur ne franchissent **pas** la passerelle
OCP — testé, `cas_4402 : Invalid access token type or some required scope is
missing`, y compris en présentant la clé client de l'application. L'accès passe
donc par le login Gigya du compte (email + mot de passe), et c'est la seule
façon de mesurer l'écart.

    pip install pyelectroluxocp
    export ELECTROLUX_USERNAME="..." ELECTROLUX_PASSWORD="..."
    python tools/ocp_probe.py --official dump.json --out ocp_dump.json

`--official` prend le fichier produit par `electrolux2mqtt.py --dump`. Sans lui,
la sonde se contente de dumper la vue OCP.

⚠️ À garder sur la branche `ocp` : ni `main`, ni l'image publiée, ni le gabarit
Community Applications.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys


def flatten(node, prefix: str = "") -> set[str]:
    """Chemins de toutes les feuilles d'un arbre de capabilities."""
    paths: set[str] = set()
    if not isinstance(node, dict):
        return paths
    for key, value in node.items():
        if key in {"values", "triggers", "default"}:
            continue
        path = f"{prefix}/{key}" if prefix else key
        if isinstance(value, dict) and ("access" in value or "type" in value):
            paths.add(path)
            paths |= flatten(value, path)
        elif isinstance(value, dict):
            paths |= flatten(value, path)
    return paths


async def probe(username: str, password: str, out: str, official: str | None) -> int:
    try:
        from pyelectroluxocp import OneAppApi
    except ImportError:
        print(
            "pyelectroluxocp n'est pas installé : pip install pyelectroluxocp\n"
            "(dépôt archivé en amont — c'est justement ce qui rend cette voie "
            "fragile)",
            file=sys.stderr,
        )
        return 1

    async with OneAppApi(username, password) as client:
        appliances = await client.get_appliances_list(include_metadata=True)
        print(f"{len(appliances)} appareil(s) vus par l'OCP")
        result = []
        for appliance in appliances:
            appliance_id = appliance.get("applianceId")
            capabilities = await client.get_appliance_capabilities(appliance_id)
            state = await client.get_appliance_state(appliance_id, include_metadata=True)
            result.append(
                {
                    "appliance": appliance,
                    "capabilities": capabilities,
                    "state": state,
                }
            )
            print(f"  {appliance_id} : {len(flatten(capabilities))} propriétés")

    with open(out, "w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, ensure_ascii=False)
    print(f"vue OCP écrite dans {out}")

    if not official:
        return 0

    with open(official, encoding="utf-8") as handle:
        reference = json.load(handle)

    print("\n=== écart entre les deux API ===")
    for item in result:
        appliance_id = item["appliance"].get("applianceId")
        match = next(
            (
                entry
                for entry in reference
                if entry["appliance"]["applianceId"] == appliance_id
            ),
            None,
        )
        if match is None:
            print(f"{appliance_id} : absent du dump officiel")
            continue
        ocp_paths = flatten(item["capabilities"])
        api_paths = flatten(match["capabilities"])
        only_ocp = sorted(ocp_paths - api_paths)
        only_api = sorted(api_paths - ocp_paths)
        print(f"\n{appliance_id}")
        print(f"  OCP : {len(ocp_paths)} propriétés, officielle : {len(api_paths)}")
        print(f"  --- seulement dans l'OCP ({len(only_ocp)}) ---")
        for path in only_ocp:
            print(f"    + {path}")
        print(f"  --- seulement dans l'officielle ({len(only_api)}) ---")
        for path in only_api:
            print(f"    - {path}")
        if not only_ocp:
            print("  => l'OCP n'apporte rien : la branche n'a pas lieu d'être.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="ocp_dump.json")
    parser.add_argument(
        "--official",
        help="dump produit par « electrolux2mqtt.py --dump », pour comparer",
    )
    args = parser.parse_args()

    username = os.environ.get("ELECTROLUX_USERNAME", "")
    password = os.environ.get("ELECTROLUX_PASSWORD", "")
    if not username or not password:
        print(
            "ELECTROLUX_USERNAME et ELECTROLUX_PASSWORD sont requis : l'API OCP "
            "n'accepte pas les jetons du portail développeur, seulement le login "
            "du compte de l'application.",
            file=sys.stderr,
        )
        return 2
    return asyncio.run(probe(username, password, args.out, args.official))


if __name__ == "__main__":
    sys.exit(main())
