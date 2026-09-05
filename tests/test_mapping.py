#!/usr/bin/env python3
"""Vérifie la traduction capabilities -> entités MQTT Discovery, hors réseau.

Ne demande ni broker ni identifiants : le jeu d'essai est un four vapeur
synthétique qui réunit les cas que le mapper doit traiter correctement.

    python tests/test_mapping.py
"""

from __future__ import annotations

import configparser
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import electrolux2mqtt as e2m  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "steam_oven.json")

failures: list[str] = []


def check(condition: bool, message: str) -> None:
    if condition:
        print(f"  ok   {message}")
    else:
        failures.append(message)
        print(f"  FAIL {message}")


def build(**options: str):
    """Construit un pont et l'appareil d'essai, sans aucune connexion."""
    fixture = json.load(open(FIXTURE, encoding="utf-8"))
    config = configparser.ConfigParser()
    for section in ("electrolux", "mqtt", "log"):
        config.add_section(section)
    for key, value in options.items():
        config.set("electrolux", key, value)
    bridge = e2m.Bridge(config)
    state = fixture["_state"]
    app = e2m.ApplianceBridge(
        appliance_id=state["applianceId"],
        slug=e2m.slugify(state["applianceId"]),
        name="Four",
        model=fixture["applianceInfo"]["model"],
        brand=fixture["applianceInfo"]["brand"],
        base_topic="electrolux/four",
        availability_topic="electrolux/four/availability",
        capabilities=fixture["capabilities"],
    )
    app.reported = dict(state["properties"]["reported"])
    app.unit = bridge._resolve_unit(app)
    bridge._build_entities(app)
    return bridge, app


print("Construction des entités")
bridge, app = build()
entities = app.entities
for key in sorted(entities):
    entity = entities[key]
    value = entity.render(app.reported) if entity.render else None
    print(f"    {entity.component:14s} {key:38s} {entity.name:28s} = {value}")

print("\nStructure")
check("userselections_steamlevel" in entities,
      "un conteneur portant lui-même un access est bien parcouru")
check("userselections" not in entities,
      "le conteneur lui-même ne devient pas une entité")
check("keymodel" not in entities, "une propriété constante est ignorée")
check(not any(k.startswith("appliancecareandmaintenance") for k in entities),
      "les compteurs de maintenance sont écartés par défaut")
check("networkinterface_startupcommand" not in entities,
      "les commandes d'administration réseau sont écartées")
check("networkinterface_linkqualityindicator" in entities,
      "la qualité du lien réseau est conservée")

print("\nUnité de température")
check(app.unit == "CELSIUS", "l'unité suit temperatureRepresentation")
check("targettemperaturec" in entities and "targettemperaturef" not in entities,
      "une seule unité est publiée")
_, app_f = build(temperature_unit="F")
check("targettemperaturef" in app_f.entities and "targettemperaturec" not in app_f.entities,
      "temperature_unit=F force le Fahrenheit")

print("\nComposants")
check(entities["program"].component == "select", "une énumération inscriptible est un select")
check(entities["program"].config["options"] == ["TRUE_FAN", "STEAM_FULL", "GRILL"],
      "les valeurs désactivées sont retirées des options")
check(entities["appliancestate"].component == "sensor", "une énumération en lecture est un sensor")
check(entities["cavitylight"].component == "switch", "un booléen inscriptible est un switch")
check(entities["doorstate"].component == "binary_sensor", "la porte est un binaire")
check(entities["doorstate"].config.get("device_class") == "door",
      "la porte garde son device_class")
check(entities["targettemperaturec"].component == "number", "une consigne est un number")
check(entities["targettemperaturec"].config["max"] == 300, "les bornes viennent des capabilities")
check(entities["displaytemperaturec"].component == "sensor", "une mesure reste un sensor")
check(entities["displaytemperaturec"].build_command is None, "une mesure n'a pas de commande")

print("\nBoutons")
check(entities["executecommand_start"].name == "Démarrer", "START devient « Démarrer »")
check(entities["executecommand_stopreset"].name == "Arrêter", "STOPRESET devient « Arrêter »")
check(entities["executecommand_start"].has_state is False, "un bouton n'a pas d'état")

print("\nValeurs publiées")
check(entities["displaytemperaturec"].render(app.reported) == "172.4", "une mesure est arrondie")
check(entities["cavitylight"].render(app.reported) == "ON", "un booléen devient ON")
check(entities["doorstate"].render(app.reported) == "OFF", "porte fermée -> OFF")
check(entities["alerts"].render(app.reported) == "42", "les alertes sont mises à plat")
check(entities["timetoend_end"].render(app.reported) != "—", "l'heure de fin est calculée")
check(entities["timetoend_end"].render({"timeToEnd": 0}) == "—", "pas d'heure de fin à l'arrêt")

print("\nCommandes")
check(entities["targettemperaturec"].build_command("185") == {"targetTemperatureC": 185.0},
      "une consigne produit le bon corps de commande")
check(entities["userselections_steamlevel"].build_command("HIGH")
      == {"userSelections": {"steamLevel": "HIGH"}},
      "une propriété imbriquée produit un corps imbriqué")
check(entities["cavitylight"].build_command("OFF") == {"cavityLight": False},
      "un switch envoie un booléen JSON")
check(entities["targetduration"].build_command("1800") == {"targetDuration": 1800},
      "une durée reste entière")
check(entities["executecommand_start"].build_command("PRESS") == {"executeCommand": "START"},
      "un bouton envoie sa valeur fixe")

print("\nMode lecture seule")
_, app_ro = build(read_only="true")
check(all(e.build_command is None for e in app_ro.entities.values()),
      "read_only=true n'expose aucune commande")
check(app_ro.entities["program"].component == "sensor",
      "read_only=true dégrade les select en sensor")

print("\nDécouverte MQTT")
payload = json.loads(
    json.dumps(
        {
            "name": entities["program"].name,
            "unique_id": f"electrolux_{app.slug}_program",
            "device": bridge._device_block(app),
            **entities["program"].config,
        }
    )
)
check(payload["device"]["identifiers"] == [f"electrolux_{app.slug}"],
      "le bloc device regroupe les entités en un seul équipement")
check(bridge._discovery_topic(app, entities["program"])
      == f"homeassistant/select/electrolux_{app.slug}/program/config",
      "le topic de découverte est bien formé")
check(app.command_topic(entities["program"]) == "electrolux/four/program/set",
      "le topic de commande est bien formé")

print(f"\n{len(entities)} entités construites")
if failures:
    print(f"\n{len(failures)} échec(s) :")
    for failure in failures:
        print(f"  - {failure}")
    sys.exit(1)
print("Tout est vert.")
