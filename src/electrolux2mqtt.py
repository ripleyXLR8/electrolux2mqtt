#!/usr/bin/env python3
"""Passerelle Electrolux Group API <-> MQTT Discovery.

Publie les états et les commandes d'un appareil Electrolux (ou AEG, Frigidaire,
+home) sur MQTT au format « MQTT Discovery » (dit aussi « HA Discovery »), que
le plugin Jeedom MQTT Discovery — ou Home Assistant — transforme tout seul en
équipement et en commandes.

Le dialogue passe par `electrolux-group-developer-sdk`, le SDK publié par
Electrolux pour son portail développeur. Les changements d'état arrivent en
**push** par Server-Sent Events : aucun polling permanent de l'API cloud.

La construction des entités est **pilotée par les capabilities** renvoyées par
l'appareil, pas par une liste écrite en dur : un four, un lave-linge ou un
réfrigérateur produisent chacun leurs propres entités, et une propriété
qu'Electrolux ajouterait demain apparaîtra sans modifier ce fichier.
"""

from __future__ import annotations

import argparse
import asyncio
import configparser
import fnmatch
import json
import logging
import os
import re
import signal
import sys
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import aiohttp
import paho.mqtt.client as mqtt
from electrolux_group_developer_sdk.auth.token_manager import TokenManager
from electrolux_group_developer_sdk.client.appliance_client import ApplianceClient
from electrolux_group_developer_sdk.client.client_exception import ApplianceClientException
from electrolux_group_developer_sdk.config import TOKEN_REFRESH_URL

LOGGER = logging.getLogger("electrolux2mqtt")

CONFIG_PATH = "/config/electrolux2mqtt.conf"
TOKEN_PATH = "/config/electrolux_token.json"

APP_VERSION = "1.0"
AUTHOR = "Richard Perez"
GITHUB = "github.com/ripleyXLR8"
EMAIL = "4702185+ripleyXLR8@users.noreply.github.com"

PAYLOAD_ON = "ON"
PAYLOAD_OFF = "OFF"
AVAILABLE = "online"
NOT_AVAILABLE = "offline"

# Libellés français des propriétés connues. Une propriété absente de cette
# table reste exposée, sous son nom technique : mieux vaut une commande au nom
# brut qu'une donnée perdue si Electrolux en ajoute une.
LABELS = {
    # Cuisson
    "program": "Programme",
    "applianceState": "État",
    "processPhase": "Phase",
    "fastHeatUpFeature": "Préchauffage rapide",
    "targetDurationEndAction": "Action en fin de durée",
    "targetFoodProbeTemperatureEndAction": "Action en fin de sonde",
    "waterTankLevel": "Réservoir d'eau",
    "waterTrayInsertionState": "Bac à eau",
    "descalingReminderState": "Détartrage à faire",
    "cleaningReminder": "Nettoyage à faire",
    "displayLight": "Luminosité de l'écran",
    "targetTemperatureC": "Consigne",
    "targetTemperatureF": "Consigne",
    "displayTemperatureC": "Température",
    "displayTemperatureF": "Température",
    "targetFoodProbeTemperatureC": "Consigne sonde",
    "targetFoodProbeTemperatureF": "Consigne sonde",
    "displayFoodProbeTemperatureC": "Température sonde",
    "displayFoodProbeTemperatureF": "Température sonde",
    "foodProbeInsertionState": "Sonde à cœur",
    "doorState": "Porte",
    "cavityLight": "Éclairage",
    "timeToEnd": "Temps restant",
    "runningTime": "Temps écoulé",
    "reminderTime": "Minuteur",
    "targetDuration": "Durée programmée",
    "startTime": "Départ différé",
    "executeCommand": "Commande",
    "waterTankState": "Réservoir d'eau",
    "descaleReminder": "Rappel de détartrage",
    "steamLevel": "Niveau de vapeur",
    # Général
    "remoteControl": "Télécommande",
    "temperatureRepresentation": "Unité de température",
    "timeFormat": "Format d'heure",
    "energySavingMode": "Mode éco",
    "silentMode": "Mode silencieux",
    "childLock": "Sécurité enfant",
    "uiLockMode": "Verrouillage clavier",
    "keySoundTone": "Sons des touches",
    "soundVolume": "Volume sonore",
    "connectivityState": "Connectivité",
    # Lavage / séchage (lave-linge, lave-vaisselle, sèche-linge)
    "cyclePhase": "Phase du cycle",
    "cycleSubPhase": "Sous-phase",
    "doorLock": "Verrou de porte",
    "stopTime": "Heure de fin",
    "analogTemperature": "Température de lavage",
    "analogSpinSpeed": "Essorage",
    "steamValue": "Vapeur",
    "drynessValue": "Niveau de séchage",
    "dryingTime": "Temps de séchage",
    "humidityTarget": "Humidité visée",
    "extraRinseNumber": "Rinçages supplémentaires",
    "timeManagerLevel": "TimeManager",
    "waterHardness": "Dureté de l'eau",
    "rinseAidLevel": "Niveau de liquide de rinçage",
    "waterUsage": "Eau consommée",
    "totalCycleCounter": "Cycles effectués",
    "totalWashCyclesCount": "Cycles de lavage",
    "totalDryCyclesCount": "Cycles de séchage",
    "totalWashingTime": "Temps de lavage cumulé",
    "totalDryingTime": "Temps de séchage cumulé",
    "applianceTotalWorkingTime": "Temps de marche cumulé",
    "endOfCycleSound": "Signal de fin de cycle",
    "tankAReserve": "Réserve bac A",
    "tankBReserve": "Réserve bac B",
    "programUID": "Programme",
    "memoryId": "Mémoire",
    "ecoScore": "Score éco",
    "energyScore": "Score énergie",
    "waterScore": "Score eau",
    # Froid
    "fridge": "Réfrigérateur",
    "freezer": "Congélateur",
    "iceMaker": "Machine à glaçons",
    "fastMode": "Mode rapide",
    "fastModeTimeToEnd": "Fin du mode rapide",
    "defrostTemperatureC": "Température de dégivrage",
    "waterFilterState": "Filtre à eau",
    "waterFilterLifeTime": "Durée de vie du filtre à eau",
    "airFilterState": "Filtre à air",
    "airFilterLifeTime": "Durée de vie du filtre à air",
    # Air (clim, purificateur, déshumidificateur)
    "ambientTemperatureC": "Température ambiante",
    "ambientTemperatureF": "Température ambiante",
    "sensorHumidity": "Humidité",
    "targetHumidity": "Humidité visée",
    "fanSpeedSetting": "Vitesse de ventilation",
    "fanSpeedState": "Ventilation",
    "filterState": "Filtre",
    "cleanAirMode": "Mode air pur",
    "waterBucketLevel": "Bac à condensats",
    "sleepMode": "Mode nuit",
    "mode": "Mode",
    "applianceMode": "Mode",
    # Table de cuisson et hotte
    "hoodFanLevel": "Vitesse de la hotte",
    "hoodGreaseFilterTimer": "Filtre à graisse",
    "hoodCharcFilterTimer": "Filtre à charbon",
    "lightIntensity": "Intensité lumineuse",
    "lightColorTemperature": "Température de couleur",
    "residualHeatState": "Chaleur résiduelle",
    "hobPotDetected": "Récipient détecté",
    "heatingQualitativeLevel": "Niveau de chauffe",
    "targetMicrowavePower": "Puissance micro-ondes",
    "waterTankEmpty": "Réservoir vide",
    "waterTrayInsertionState": "Bac à eau",
    # Robot aspirateur
    "robotStatus": "État du robot",
    "dustbinStatus": "Bac à poussière",
    "batteryStatus": "Batterie",
    "powerMode": "Puissance d'aspiration",
    # Réseau
    "linkQualityIndicator": "Qualité du signal",
    "swVersion": "Version logicielle",
    "otaState": "Mise à jour",
    "networkInterface": "Réseau",
    "alerts": "Alertes",
}

# Verbes des commandes en écriture seule : elles deviennent des boutons, et
# « Démarrer » se lit mieux que « Commande Start » sur un tableau de bord.
COMMAND_LABELS = {
    "START": "Démarrer",
    "STOP": "Arrêter",
    "STOPRESET": "Arrêter",
    "PAUSE": "Mettre en pause",
    "RESUME": "Reprendre",
    "ON": "Allumer",
    "OFF": "Éteindre",
    "DOCK": "Retour à la base",
    "HOME": "Retour à la base",
    "RESET": "Réinitialiser",
}

# Libellés des conteneurs (cavités d'un four, zones, sous-ensembles). Sert de
# préfixe au nom de l'entité quand l'appareil en déclare plusieurs.
CONTAINER_LABELS = {
    "upperOven": "Four haut",
    "bottomOven": "Four bas",
    "extraCavity": "Cavité supplémentaire",
    "oven": "Four",
    "cavity": "Cavité",
    "networkInterface": "Réseau",
    "hood": "Hotte",
    "hobHood": "Hotte",
    "fridge": "Réfrigérateur",
    "freezer": "Congélateur",
    "iceMaker": "Machine à glaçons",
    "userSelections": "Options",
    "autoDosing": "Dosage automatique",
    "miscellaneousState": "Divers",
    "fCMiscellaneousState": "Divers",
    "airConditioner": "Climatiseur",
}

# Conteneurs représentant une cavité de cuisson : quand c'est le seul de
# l'appareil, ses entités n'ont pas besoin d'être préfixées.
CAVITY_CONTAINERS = {"upperOven", "bottomOven", "oven", "cavity", "extraCavity"}

# Unités déduites du nom de la propriété, faute d'unité dans les capabilities.
UNITS = {
    "targetTemperatureC": "°C",
    "displayTemperatureC": "°C",
    "targetFoodProbeTemperatureC": "°C",
    "displayFoodProbeTemperatureC": "°C",
    "targetTemperatureF": "°F",
    "displayTemperatureF": "°F",
    "targetFoodProbeTemperatureF": "°F",
    "displayFoodProbeTemperatureF": "°F",
    "ambientTemperatureC": "°C",
    "ambientTemperatureF": "°F",
    "defrostTemperatureC": "°C",
    "defrostTemperatureF": "°F",
    "sensorHumidity": "%",
    "targetHumidity": "%",
    "batteryStatus": "%",
    "waterUsage": "L",
    "timeToEnd": "s",
    "runningTime": "s",
    "targetDuration": "s",
    "startTime": "s",
    "stopTime": "s",
    "fastModeTimeToEnd": "s",
    "totalWashingTime": "s",
    "totalDryingTime": "s",
    "applianceTotalWorkingTime": "s",
    "compressorCoolingRuntime": "s",
    "compressorHeatingRuntime": "s",
    "totalRuntime": "s",
    "maxTimerDuration": "s",
    "reminderTime": "s",
}

# Propriétés exprimées en secondes : bornes par défaut d'un composant number
# quand les capabilities n'en fournissent pas.
DURATION_PROPERTIES = {
    "timeToEnd",
    "runningTime",
    "targetDuration",
    "startTime",
    "stopTime",
    "reminderTime",
    "maxTimerDuration",
    "dryingTime",
}

# Propriétés binaires reconnues : valeur « vraie » de chaque énumération.
BINARY_TRUE = {
    "doorState": "OPEN",
    "waterTankEmpty": "EMPTY",
}

DEVICE_CLASSES = {
    "doorState": "door",
    "sensorHumidity": "humidity",
    "batteryStatus": "battery",
}

# Motifs (style shell) écartés par défaut. On y trouve l'administration de la
# passerelle réseau — dont certaines commandes sont carrément dangereuses
# (désinstallation, ré-appairage) — et les internes d'algorithmes que les
# appareils exposent par centaines : compteurs de maintenance, paramètres du
# « Dry What You Wash », mémoires de cycle. Réglable par ELECTROLUX_EXCLUDE,
# et ELECTROLUX_INCLUDE permet d'en repêcher au cas par cas.
DEFAULT_EXCLUDE = (
    # Tout le sous-arbre réseau sauf ce que DEFAULT_INCLUDE rattrape : chaque
    # appareil nomme ses propriétés de mise à jour à sa façon (otaState ici,
    # oTA3State là), une liste noire ne tiendrait pas.
    "networkInterface/*",
    "applianceCareAndMaintenance*",
    "dwyw*",
    "cyclePersonalization*",
    "cycleMemory*",
    "CustomPlay*",
    "*/maint*",
    "humanCentricLightEventSettings*",
    "miscellaneous/*",
    # Favoris et recettes : structures imbriquées propres à l'écran de
    # l'appareil, sans usage domotique.
    "favorite*",
    "*/favorite",
    "*/favorite/*",
    "messageQueueSync*",
    "*/messageQueueSync*",
    # Réglages d'interface et de localisation : ils se font sur l'appareil, et
    # une liste déroulante de 26 langues n'a rien à faire sur un tableau de bord.
    "language",
    "clockStyle",
    "cpv",
    "applianceLocalTimeOffset",
    "autoLocalTimeOffset",
    "localTimeAutomaticMode",
    "timeZoneDatabaseName",
)

# Repêchées malgré les exclusions par défaut. Une exclusion posée par
# l'utilisateur l'emporte quand même sur cette liste.
DEFAULT_INCLUDE = (
    "networkInterface/linkQualityIndicator",
    "networkInterface/swVersion",
)

# Valeur entière que les appareils publient pour « non réglé » : c'est INT_MIN,
# et la laisser passer polluerait durablement un historique Jeedom.
NOT_SET = -2147483648

# Types de capabilities considérés comme numériques.
NUMERIC_TYPES = {"number", "int", "temperature", "float"}

# Types portant une liste d'alertes actives.
ALERT_TYPES = {"alert", "alerts"}

# Types structurés qui, faute d'enfants déclarés, ne donneraient qu'une entité
# affichant un dictionnaire brut : sans intérêt sur un tableau de bord.
OPAQUE_TYPES = {"complex", "container", "map", "custom", "object", "careMaintenance"}

# Le SSE ne porte que des deltas : ces clés d'enveloppe sont ignorées au merge.
EVENT_ENVELOPE_KEYS = {"applianceId", "timestamp", "eventType", "type"}


def slugify(value: str) -> str:
    """Réduit une chaîne à un identifiant utilisable dans un topic MQTT."""
    text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_").lower()
    return text or "appliance"


def camel_to_words(value: str) -> str:
    """« targetTemperatureC » -> « Target temperature c », pour les inconnues."""
    text = re.sub(r"(?<!^)(?=[A-Z])", " ", value).strip()
    return text[:1].upper() + text[1:].lower()


def format_alerts(value: Any) -> str:
    """Met une liste d'alertes à plat : « 36, E42 », ou « — » si tout va bien."""
    if not isinstance(value, list) or not value:
        return "—"
    codes = []
    for alert in value:
        if isinstance(alert, dict):
            codes.append(str(alert.get("code", alert.get("name", "?"))))
        else:
            codes.append(str(alert))
    return ", ".join(codes)


def deep_merge(base: dict[str, Any], delta: dict[str, Any]) -> None:
    """Fusionne récursivement un delta dans l'état courant, sur place."""
    for key, value in delta.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value


def deep_get(data: dict[str, Any], path: tuple[str, ...]) -> Any:
    """Lit une valeur imbriquée, None si le chemin n'existe pas."""
    current: Any = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


@dataclass
class Entity:
    """Une entité MQTT Discovery : un état publié, et parfois une commande."""

    key: str
    component: str
    name: str
    config: dict[str, Any]
    # Chemin de la propriété dans l'état « reported ». Vide pour les entités
    # dérivées (témoin de liaison, heure de fin, alertes).
    path: tuple[str, ...] = ()
    render: Callable[[dict[str, Any]], str | None] | None = None
    # Traduit la charge utile MQTT reçue en corps de commande pour l'API.
    build_command: Callable[[str], dict[str, Any]] | None = None
    has_state: bool = True


@dataclass
class ApplianceBridge:
    """Tout ce qui concerne un appareil : son état courant et ses entités."""

    appliance_id: str
    slug: str
    name: str
    model: str
    brand: str
    base_topic: str
    availability_topic: str
    capabilities: dict[str, Any] = field(default_factory=dict)
    reported: dict[str, Any] = field(default_factory=dict)
    entities: dict[str, Entity] = field(default_factory=dict)
    online: bool = False
    # Unité de température retenue pour cet appareil : les appareils exposent
    # souvent la même grandeur en °C et en °F, publier les deux doublerait les
    # entités.
    unit: str = "CELSIUS"

    def state_topic(self, entity: Entity) -> str:
        return f"{self.base_topic}/{entity.key}/state"

    def command_topic(self, entity: Entity) -> str:
        return f"{self.base_topic}/{entity.key}/set"


class Bridge:
    """Le pont : une connexion MQTT, un flux SSE pour tous les appareils."""

    def __init__(self, config: configparser.ConfigParser) -> None:
        self._config = config
        self._discovery_prefix = config.get(
            "mqtt", "discovery_prefix", fallback="homeassistant"
        )
        self._topic_prefix = config.get("mqtt", "topic_prefix", fallback="electrolux")
        self._refresh_interval = config.getint(
            "electrolux", "refresh_interval", fallback=900
        )
        # Cadence effective de la relecture : resserrée si le flux SSE se révèle
        # indisponible et qu'il faut se rabattre sur du polling.
        self._poll_interval = self._refresh_interval
        self._read_only = config.getboolean("electrolux", "read_only", fallback=False)
        self._unit_pref = config.get(
            "electrolux", "temperature_unit", fallback="auto"
        ).strip().upper()
        excluded = config.get("electrolux", "exclude", fallback="").strip()
        self._user_exclude = {
            item.strip() for item in excluded.split(",") if item.strip()
        }
        included = config.get("electrolux", "include", fallback="").strip()
        self._include = {item.strip() for item in included.split(",") if item.strip()}
        self._token_file = config.get("electrolux", "token_file", fallback=TOKEN_PATH)
        self._client: ApplianceClient | None = None
        self._tokens: TokenManager | None = None
        self._mqtt: mqtt.Client | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._appliances: dict[str, ApplianceBridge] = {}
        self._commands: dict[str, tuple[ApplianceBridge, Entity]] = {}
        self._stopping = asyncio.Event()

    # ------------------------------------------------------------------
    # Jetons : ils tournent à chaque rafraîchissement, donc ils se persistent
    # ------------------------------------------------------------------

    def _load_tokens(self) -> tuple[str, str, str]:
        """Retourne (clé API, access token, refresh token), fichier prioritaire.

        Le portail Electrolux délivre un couple de jetons, et l'API en renvoie
        un **nouveau** à chaque rafraîchissement : sans persistance, un simple
        redémarrage du conteneur repartirait d'un refresh token déjà consommé.
        """
        api_key = self._config.get("electrolux", "api_key", fallback="").strip()
        access = self._config.get("electrolux", "access_token", fallback="").strip()
        refresh = self._config.get("electrolux", "refresh_token", fallback="").strip()

        try:
            with open(self._token_file, encoding="utf-8") as handle:
                saved = json.load(handle)
        except FileNotFoundError:
            return api_key, access, refresh
        except (OSError, ValueError) as err:
            LOGGER.warning("Fichier de jetons illisible (%s) : %s", self._token_file, err)
            return api_key, access, refresh

        if saved.get("api_key") and saved["api_key"] != api_key:
            LOGGER.info("Clé API changée dans la configuration : jetons sauvegardés ignorés")
            return api_key, access, refresh
        LOGGER.info("Jetons repris depuis %s", self._token_file)
        return (
            api_key,
            saved.get("access_token") or access,
            saved.get("refresh_token") or refresh,
        )

    def _save_tokens(self, access_token: str, refresh_token: str, api_key: str) -> None:
        """Callback du SDK : appelé à chaque rotation des jetons."""
        payload = {
            "api_key": api_key,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "saved_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        try:
            directory = os.path.dirname(self._token_file) or "."
            os.makedirs(directory, exist_ok=True)
            tmp = f"{self._token_file}.tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
            os.replace(tmp, self._token_file)
            os.chmod(self._token_file, 0o600)
        except OSError as err:
            LOGGER.error(
                "Impossible d'écrire les jetons dans %s : %s — le prochain "
                "redémarrage repartira du refresh token de la configuration, "
                "qui aura peut-être expiré. Monter un dossier /config.",
                self._token_file,
                err,
            )

    async def _bootstrap_access_token(self, refresh_token: str) -> str:
        """Échange un refresh token contre un access token, au premier démarrage."""
        async with aiohttp.ClientSession() as session:
            async with session.post(
                TOKEN_REFRESH_URL, json={"refreshToken": refresh_token}
            ) as response:
                if response.status >= 400:
                    body = await response.text()
                    raise SystemExit(
                        "Refresh token refusé par Electrolux "
                        f"(HTTP {response.status}) : {body.strip()[:200]}\n"
                        "Regénérer le couple de jetons sur "
                        "https://developer.electrolux.one/dashboard"
                    )
                data = await response.json()
        return data["accessToken"]

    # ------------------------------------------------------------------
    # Construction des entités à partir des capabilities de l'appareil
    # ------------------------------------------------------------------

    def _is_leaf(self, node: Any) -> bool:
        """Une feuille décrit une propriété ; un conteneur en contient d'autres.

        Attention : un conteneur porte souvent lui-même un `access` et un `type`
        (`userSelections` est déclaré « complex / readwrite » tout en ayant des
        enfants). Ce qui tranche, c'est la présence d'enfants décrivant eux-mêmes
        une propriété — les valeurs d'une énumération, elles, n'ont ni `access`
        ni `type`.
        """
        if not isinstance(node, dict):
            return False
        if not ("access" in node or "type" in node):
            return False
        for key, value in node.items():
            if key in {"values", "triggers", "default"}:
                continue
            if isinstance(value, dict) and ("access" in value or "type" in value):
                return False
        return True

    def _is_container(self, node: Any) -> bool:
        """Vrai si le nœud contient au moins une propriété."""
        if not isinstance(node, dict):
            return False
        return any(
            key not in {"values", "triggers", "default"}
            and (self._is_leaf(value) or self._is_container(value))
            for key, value in node.items()
        )

    def _excluded(self, path: str) -> bool:
        """Priorités : inclusion de l'utilisateur, puis son exclusion, puis les
        listes par défaut. Ce qu'il écrit l'emporte toujours sur les réglages
        d'usine, dans un sens comme dans l'autre."""
        def matches(patterns) -> bool:
            return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)

        if matches(self._include):
            return False
        if matches(self._user_exclude):
            return True
        if matches(DEFAULT_INCLUDE):
            return False
        return matches(DEFAULT_EXCLUDE)

    def _walk(
        self, node: dict[str, Any], prefix: tuple[str, ...] = ()
    ) -> list[tuple[tuple[str, ...], dict[str, Any]]]:
        """Aplatit l'arbre des capabilities en (chemin, définition)."""
        leaves: list[tuple[tuple[str, ...], dict[str, Any]]] = []
        for key, value in node.items():
            if key in {"values", "triggers", "default"}:
                continue
            path = prefix + (key,)
            if self._is_leaf(value):
                leaves.append((path, value))
            elif isinstance(value, dict):
                leaves.extend(self._walk(value, path))
        return leaves

    def _prefix(self, path: tuple[str, ...], containers: list[str]) -> str:
        """« Four bas — » quand l'appareil a plusieurs cavités, sinon rien.

        Sur un four simple, « Four bas — Température » n'apporte que du bruit ;
        sur un four double ou une table à huit zones, le préfixe est ce qui rend
        les entités distinguables.
        """
        if len(path) <= 1:
            return ""
        container = path[0]
        # Seul cas où le préfixe est du bruit : l'appareil n'a qu'une cavité,
        # et tout ce qu'il expose lui appartient déjà.
        if container in CAVITY_CONTAINERS and containers == [container]:
            return ""
        label = CONTAINER_LABELS.get(container, camel_to_words(container))
        return f"{label} — "

    def _label(self, path: tuple[str, ...], containers: list[str]) -> str:
        """Nom lisible d'une entité, préfixé par la cavité s'il y en a plusieurs."""
        prop = path[-1]
        return self._prefix(path, containers) + (LABELS.get(prop) or camel_to_words(prop))

    def _unit_matches(self, prop: str, unit: str) -> bool:
        """Écarte les propriétés de l'unité de température non retenue."""
        if prop.endswith("C") and prop[:-1].lower().endswith("temperature"):
            return unit != "FAHRENHEIT"
        if prop.endswith("F") and prop[:-1].lower().endswith("temperature"):
            return unit == "FAHRENHEIT"
        return True

    def _render_leaf(self, path: tuple[str, ...], node: dict[str, Any]):
        """Fabrique la fonction qui met en forme la valeur pour MQTT."""
        prop = path[-1]
        kind = node.get("type", "string")
        is_binary = kind == "boolean" or prop in BINARY_TRUE
        true_value = BINARY_TRUE.get(prop)

        def render(reported: dict[str, Any]) -> str | None:
            value = deep_get(reported, path)
            if kind in ALERT_TYPES:
                return format_alerts(value)
            if value is None:
                return None
            if is_binary:
                if true_value is not None:
                    return PAYLOAD_ON if value == true_value else PAYLOAD_OFF
                return PAYLOAD_ON if value else PAYLOAD_OFF
            if isinstance(value, bool):
                return PAYLOAD_ON if value else PAYLOAD_OFF
            if isinstance(value, (int, float)) and kind in NUMERIC_TYPES:
                # « Non réglé » : ne rien publier plutôt que de faire entrer
                # -2147483648 dans l'historique.
                if value == NOT_SET:
                    return None
                rounded = round(float(value), 1)
                return str(int(rounded)) if rounded.is_integer() else str(rounded)
            return str(value)

        return render

    def _command_builder(self, path: tuple[str, ...], node: dict[str, Any], fixed: Any = None):
        """Fabrique la fonction qui traduit une charge MQTT en corps d'API.

        Le corps reprend la forme du chemin : {"bottomOven": {"program": "BAKE"}}
        pour une propriété de cavité, {"energySavingMode": "ON"} à la racine.
        """
        kind = node.get("type", "string")
        integer = all(
            isinstance(node.get(bound), int) or node.get(bound) is None
            for bound in ("min", "max", "step")
        ) and kind in {"number", "int"}

        def convert(payload: str) -> Any:
            if fixed is not None:
                return fixed
            if kind == "boolean":
                return payload.strip().upper() in {"ON", "TRUE", "1"}
            if kind in NUMERIC_TYPES:
                number = float(payload)
                return int(number) if integer else round(number, 1)
            return payload.strip()

        def build(payload: str) -> dict[str, Any]:
            body: Any = convert(payload)
            for key in reversed(path):
                body = {key: body}
            return body

        return build

    def _numeric_bounds(
        self, joined: str, node: dict[str, Any], ranges: dict[str, dict[str, float]]
    ) -> dict[str, float]:
        """Bornes d'un curseur : capability, sinon énumération, sinon programmes."""
        bounds = {
            key: node[key] for key in ("min", "max", "step") if node.get(key) is not None
        }
        if "min" in bounds and "max" in bounds:
            return bounds

        # Une énumération numérique (volume sonore 1..4) porte ses bornes dans
        # ses propres valeurs.
        values = node.get("values")
        if isinstance(values, dict) and values:
            try:
                numbers = sorted(float(name) for name in values)
            except ValueError:
                numbers = []
            if numbers:
                bounds.setdefault("min", numbers[0])
                bounds.setdefault("max", numbers[-1])
                bounds.setdefault("step", 1)
                return bounds

        bounds.update({k: v for k, v in ranges.get(joined, {}).items() if k not in bounds})
        return bounds

    def _program_ranges(self, capabilities: dict[str, Any]) -> dict[str, dict[str, float]]:
        """Plages des consignes, glanées dans les métadonnées des programmes.

        Un four ne déclare aucun min/max sur `targetTemperatureC` : la plage
        dépend du mode de cuisson et vit dans les métadonnées de chaque valeur
        de `program` (80-230 °C en chaleur tournante, 50-100 °C en vapeur…).
        On en prend l'union, ce qui donne des bornes justes pour un curseur quel
        que soit le programme en cours, sans avoir à republier la découverte à
        chaque changement de mode.
        """
        ranges: dict[str, dict[str, float]] = {}
        for _path, node in self._walk(capabilities):
            values = node.get("values")
            if not isinstance(values, dict):
                continue
            for meta in values.values():
                if not isinstance(meta, dict):
                    continue
                for target, spec in meta.items():
                    if not isinstance(spec, dict) or spec.get("disabled"):
                        continue
                    current = ranges.setdefault(target, {})
                    for bound, pick in (("min", min), ("max", max)):
                        value = spec.get(bound)
                        if isinstance(value, (int, float)):
                            current[bound] = pick(current.get(bound, value), value)
                    step = spec.get("step")
                    # Un pas nul veut dire « température imposée » : il ne doit
                    # pas écraser le pas réel des autres programmes.
                    if isinstance(step, (int, float)) and step > 0:
                        current["step"] = min(current.get("step", step), step)
        return {key: value for key, value in ranges.items() if value}

    def _values(self, node: dict[str, Any]) -> list[str]:
        """Valeurs autorisées d'une énumération, les désactivées en moins."""
        values = node.get("values")
        if not isinstance(values, dict):
            return []
        return [
            name
            for name, meta in values.items()
            if not (isinstance(meta, dict) and meta.get("disabled"))
        ]

    def _build_entities(self, app: ApplianceBridge) -> None:
        """Traduit les capabilities de l'appareil en entités MQTT Discovery."""
        containers = [
            key
            for key, value in app.capabilities.items()
            if self._is_container(value) and key != "networkInterface"
        ]
        ranges = self._program_ranges(app.capabilities)
        for path, node in self._walk(app.capabilities):
            joined = "/".join(path)
            if self._excluded(joined):
                LOGGER.debug("Propriété écartée : %s", joined)
                continue
            prop = path[-1]
            if not self._unit_matches(prop, app.unit):
                continue
            access = node.get("access", "read")
            kind = node.get("type", "string")
            # « constant » décrit une caractéristique figée de l'appareil
            # (modèle, seuils d'usine) : ce n'est pas un état à suivre.
            if access == "constant":
                continue
            if kind in OPAQUE_TYPES:
                LOGGER.debug("Propriété structurée sans enfants ignorée : %s", joined)
                continue
            values = self._values(node)
            key = slugify(joined)
            name = self._label(path, containers)
            writable = access in {"write", "readwrite"} and not self._read_only

            if access == "write":
                # Écriture seule : une énumération devient un bouton par valeur,
                # ce qui donne des commandes « action » directes côté Jeedom.
                if not writable or not values:
                    continue
                prefix = self._prefix(path, containers)
                for value in values:
                    verb = COMMAND_LABELS.get(value)
                    button_name = (
                        f"{prefix}{verb}"
                        if verb
                        else f"{name} {value.title().replace('_', ' ')}"
                    )
                    self._add(
                        app,
                        Entity(
                            key=f"{key}_{slugify(value)}",
                            component="button",
                            name=button_name,
                            config={"payload_press": value},
                            has_state=False,
                            build_command=self._command_builder(path, node, fixed=value),
                        ),
                    )
                continue

            render = self._render_leaf(path, node)
            build = self._command_builder(path, node) if writable else None

            if kind == "boolean":
                component = "switch" if writable else "binary_sensor"
                config: dict[str, Any] = {
                    "payload_on": PAYLOAD_ON,
                    "payload_off": PAYLOAD_OFF,
                }
                if component == "switch":
                    config |= {"state_on": PAYLOAD_ON, "state_off": PAYLOAD_OFF}
            elif prop in BINARY_TRUE:
                component = "binary_sensor"
                config = {"payload_on": PAYLOAD_ON, "payload_off": PAYLOAD_OFF}
                build = None
            elif kind in NUMERIC_TYPES and writable:
                component = "number"
                default_max = 86400 if prop in DURATION_PROPERTIES else 300
                default_step = 60 if prop in DURATION_PROPERTIES else 1
                bounds = self._numeric_bounds(joined, node, ranges)
                config = {
                    "min": bounds.get("min", 0),
                    "max": bounds.get("max", default_max),
                    "step": bounds.get("step", default_step),
                    "mode": "box",
                }
                unit = UNITS.get(prop)
                if unit:
                    config["unit_of_measurement"] = unit
            elif values and writable:
                component = "select"
                config = {"options": values}
            else:
                component = "sensor"
                config = {}
                unit = UNITS.get(prop)
                if unit:
                    config["unit_of_measurement"] = unit
                build = None

            device_class = DEVICE_CLASSES.get(prop)
            if device_class and component in {"binary_sensor", "sensor"}:
                config["device_class"] = device_class

            self._add(
                app,
                Entity(
                    key=key,
                    component=component,
                    name=name,
                    config=config,
                    path=path,
                    render=render,
                    build_command=build,
                ),
            )

        self._add_derived(app, containers)
        self._dedupe_device_classes(app)
        self._resolve_name_collisions(app)

    def _add(self, app: ApplianceBridge, entity: Entity) -> None:
        if entity.key in app.entities:
            LOGGER.warning("Entité en double ignorée : %s", entity.key)
            return
        app.entities[entity.key] = entity

    def _add_derived(self, app: ApplianceBridge, containers: list[str]) -> None:
        """Entités calculées : alertes, heure de fin, témoin de liaison."""
        # Certains appareils remontent des alertes sans les déclarer en
        # capability : on les expose quand même.
        if "alerts" in app.reported and "alerts" not in app.entities:
            def render_alerts(reported: dict[str, Any]) -> str:
                return format_alerts(reported.get("alerts"))

            self._add(
                app,
                Entity(
                    key="alerts",
                    component="sensor",
                    name="Alertes",
                    config={},
                    path=("alerts",),
                    render=render_alerts,
                ),
            )

        # Heure de fin : le « temps restant » brut en secondes est illisible sur
        # une tuile, l'heure à laquelle ce sera prêt ne l'est pas.
        for path, _node in self._walk(app.capabilities):
            if path[-1] != "timeToEnd" or self._excluded("/".join(path)):
                continue
            label = self._prefix(path, containers) + "Fin de cycle"

            def render_end(reported: dict[str, Any], path: tuple[str, ...] = path) -> str:
                remaining = deep_get(reported, path)
                if not isinstance(remaining, (int, float)) or remaining <= 0:
                    return "—"
                end = datetime.now().astimezone() + timedelta(seconds=float(remaining))
                return end.strftime("%H:%M")

            self._add(
                app,
                Entity(
                    key=slugify("/".join(path) + "_end"),
                    component="sensor",
                    name=label,
                    config={},
                    path=path,
                    render=render_end,
                ),
            )

        # Témoin de liaison : visible même quand l'appareil est injoignable,
        # et rabattu par le testament MQTT si la passerelle meurt.
        self._add(
            app,
            Entity(
                key="link",
                component="binary_sensor",
                name="En ligne",
                config={
                    "device_class": "connectivity",
                    "payload_on": PAYLOAD_ON,
                    "payload_off": PAYLOAD_OFF,
                },
                render=lambda reported: None,
            ),
        )

    def _resolve_name_collisions(self, app: ApplianceBridge) -> None:
        """Distingue deux entités homonymes, sans toucher à la plus parlante.

        Un four expose `applianceState` deux fois : à la racine (l'appareil est
        allumé ou non) et dans la cavité (l'état de la cuisson). Les deux
        s'appellent « État ». C'est celle de la cavité qu'on regarde au
        quotidien : c'est donc celle de la racine qui est qualifiée.
        """
        by_name: dict[str, list[Entity]] = {}
        for entity in app.entities.values():
            by_name.setdefault(entity.name, []).append(entity)
        for name, group in by_name.items():
            if len(group) < 2:
                continue
            for entity in group:
                if len(entity.path) == 1:
                    entity.name = f"{name} (appareil)"
                    LOGGER.debug("Homonymie résolue : %s -> %s", name, entity.name)

    def _dedupe_device_classes(self, app: ApplianceBridge) -> None:
        """Retire les device_class utilisées plus d'une fois sur l'appareil.

        Le plugin Jeedom MQTT Discovery **renomme la commande d'après le
        device_class** : deux entités partageant la même classe se retrouvent
        nommées « Porte » et « Porte (1) », et le libellé métier est perdu.
        """
        counts: dict[str, int] = {}
        for entity in app.entities.values():
            klass = entity.config.get("device_class")
            if klass:
                counts[klass] = counts.get(klass, 0) + 1
        for entity in app.entities.values():
            klass = entity.config.get("device_class")
            if klass and counts.get(klass, 0) > 1 and entity.key != "link":
                LOGGER.debug(
                    "device_class %s retirée de %s (utilisée %d fois)",
                    klass,
                    entity.key,
                    counts[klass],
                )
                entity.config.pop("device_class", None)

    # ------------------------------------------------------------------
    # Découverte MQTT
    # ------------------------------------------------------------------

    def _discovery_topic(self, app: ApplianceBridge, entity: Entity) -> str:
        return (
            f"{self._discovery_prefix}/{entity.component}"
            f"/electrolux_{app.slug}/{entity.key}/config"
        )

    def _device_block(self, app: ApplianceBridge) -> dict[str, Any]:
        block: dict[str, Any] = {
            "identifiers": [f"electrolux_{app.slug}"],
            "name": app.name,
            "manufacturer": app.brand or "Electrolux",
        }
        if app.model:
            block["model"] = app.model
        return block

    def _publish_discovery(self, app: ApplianceBridge) -> None:
        """(Re)publie les messages de découverte de toutes les entités."""
        for entity in app.entities.values():
            payload: dict[str, Any] = {
                "name": entity.name,
                "unique_id": f"electrolux_{app.slug}_{entity.key}",
                "object_id": f"electrolux_{app.slug}_{entity.key}",
                "device": self._device_block(app),
                **entity.config,
            }
            if entity.has_state:
                payload["state_topic"] = app.state_topic(entity)
            if entity.build_command is not None:
                payload["command_topic"] = app.command_topic(entity)
            # Le témoin de liaison doit rester lisible quand le lien est coupé :
            # il est le seul à ne pas dépendre de la disponibilité.
            if entity.key != "link":
                payload["availability_topic"] = app.availability_topic
                payload["payload_available"] = AVAILABLE
                payload["payload_not_available"] = NOT_AVAILABLE
            self._publish(
                self._discovery_topic(app, entity),
                json.dumps(payload, ensure_ascii=False),
                retain=True,
            )

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------

    def _publish(self, topic: str, payload: str, retain: bool = True) -> None:
        if self._mqtt is None:
            return
        LOGGER.debug("MQTT -> %s = %s", topic, payload)
        self._mqtt.publish(topic, payload, qos=1, retain=retain)

    def _publish_states(self, app: ApplianceBridge) -> None:
        """Republie tous les états connus (après reconnexion notamment)."""
        for entity in app.entities.values():
            if entity.render is None or not entity.has_state:
                continue
            value = entity.render(app.reported)
            if value is not None:
                self._publish(app.state_topic(entity), value)
        self._publish_link(app)

    def _publish_availability(self, app: ApplianceBridge) -> None:
        self._publish(
            app.availability_topic, AVAILABLE if app.online else NOT_AVAILABLE
        )

    def _publish_link(self, app: ApplianceBridge) -> None:
        entity = app.entities.get("link")
        if entity is not None:
            self._publish(
                app.state_topic(entity), PAYLOAD_ON if app.online else PAYLOAD_OFF
            )

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        """Rejoue découverte, souscriptions et états à *chaque* connexion.

        En session non persistante le broker oublie les souscriptions à chaque
        coupure : tout doit être rejoué ici, sinon la passerelle continue de
        publier mais ne reçoit plus aucune commande.
        """
        if reason_code != 0:
            LOGGER.error("Connexion MQTT refusée : %s", reason_code)
            return
        LOGGER.info("Connecté au broker MQTT, (re)publication de la découverte")
        for app in self._appliances.values():
            self._publish_discovery(app)
            self._publish_availability(app)
            self._publish_states(app)
        for topic in self._commands:
            client.subscribe(topic, qos=1)
            LOGGER.debug("Souscription à %s", topic)
        LOGGER.info("%d topic(s) de commande souscrit(s)", len(self._commands))

    def _on_disconnect(self, client, userdata, *args):
        LOGGER.warning("Déconnexion du broker MQTT, reconnexion automatique")

    def _on_message(self, client, userdata, message):
        topic = message.topic
        payload = message.payload.decode("utf-8", "replace").strip()
        entry = self._commands.get(topic)
        if entry is None or self._loop is None:
            return
        app, entity = entry
        LOGGER.info("Commande reçue : %s = %s", entity.name, payload)
        asyncio.run_coroutine_threadsafe(
            self._run_command(app, entity, payload), self._loop
        )

    async def _run_command(
        self, app: ApplianceBridge, entity: Entity, payload: str
    ) -> None:
        assert entity.build_command is not None
        assert self._client is not None
        try:
            body = entity.build_command(payload)
        except (ValueError, TypeError) as err:
            LOGGER.error("Valeur invalide pour %s : %r (%s)", entity.name, payload, err)
            return
        try:
            await self._client.send_command(app.appliance_id, body)
        except ApplianceClientException as err:
            remote = deep_get(app.reported, ("remoteControl",))
            hint = ""
            if remote and remote != "ENABLED":
                hint = (
                    f" — la télécommande de l'appareil est sur « {remote} » : "
                    "l'activer sur l'écran de l'appareil (elle se coupe seule "
                    "après un moment, c'est une contrainte de sécurité)"
                )
            LOGGER.error("Échec de la commande %s = %s : %s%s", entity.name, payload, err, hint)
            return
        LOGGER.info("Commande %s = %s acceptée", entity.name, payload)
        # Retour immédiat sans attendre la confirmation du flux SSE : l'état
        # réel écrasera cette valeur au prochain évènement.
        if entity.has_state and entity.path:
            deep_merge(app.reported, body)
            if entity.render is not None:
                value = entity.render(app.reported)
                if value is not None:
                    self._publish(app.state_topic(entity), value)

    def _connect_mqtt(self) -> mqtt.Client:
        cfg = self._config
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=cfg.get("mqtt", "client_id", fallback="electrolux2mqtt"),
        )
        user = cfg.get("mqtt", "login", fallback="").strip()
        if user:
            client.username_pw_set(user, cfg.get("mqtt", "password", fallback=""))
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        # Testament : si la passerelle meurt, le témoin « En ligne » retombe
        # tout seul. Le protocole n'autorise qu'un testament par connexion,
        # donc seul le premier appareil en bénéficie.
        first = next(iter(self._appliances.values()), None)
        if first is not None:
            link = first.entities.get("link")
            if link is not None:
                client.will_set(
                    first.state_topic(link), PAYLOAD_OFF, qos=1, retain=True
                )
        if len(self._appliances) > 1:
            LOGGER.warning(
                "%d appareils : seul %s a un testament MQTT",
                len(self._appliances),
                first.appliance_id if first else "-",
            )
        client.connect_async(
            cfg.get("mqtt", "host", fallback="127.0.0.1"),
            cfg.getint("mqtt", "port", fallback=1883),
            keepalive=60,
        )
        client.loop_start()
        return client

    # ------------------------------------------------------------------
    # Flux d'états
    # ------------------------------------------------------------------

    def _apply_state(self, app: ApplianceBridge, state: Any) -> None:
        """Absorbe un état complet renvoyé par l'API REST."""
        reported = (state.properties or {}).get("reported") or {}
        deep_merge(app.reported, reported)
        app.online = (state.connectionState or "").lower() == "connected"
        self._publish_availability(app)
        self._publish_states(app)

    def _extract_reported(self, event: dict[str, Any]) -> dict[str, Any]:
        """Extrait le delta d'un évènement SSE, quelle que soit son enveloppe."""
        properties = event.get("properties")
        if isinstance(properties, dict):
            reported = properties.get("reported")
            if isinstance(reported, dict):
                return reported
            return {k: v for k, v in properties.items() if k != "desired"}
        reported = event.get("reported")
        if isinstance(reported, dict):
            return reported
        return {k: v for k, v in event.items() if k not in EVENT_ENVELOPE_KEYS}

    def _on_event(self, app: ApplianceBridge, event: dict[str, Any]) -> None:
        """Callback SSE — exécuté dans la boucle asyncio du SDK."""
        LOGGER.debug("Évènement %s : %s", app.appliance_id, json.dumps(event)[:800])
        delta = self._extract_reported(event)
        if not delta:
            return
        deep_merge(app.reported, delta)
        connection = event.get("connectionState") or delta.get("connectivityState")
        if isinstance(connection, str):
            app.online = connection.lower() == "connected"
            self._publish_availability(app)
        # Republier tout l'appareil coûte quelques messages retenus mais évite
        # de rater une entité dérivée (heure de fin, alertes) qu'un delta
        # partiel aurait laissée en arrière.
        self._publish_states(app)

    async def _stream(self) -> None:
        """Flux SSE global : le SDK gère lui-même reconnexion et backoff.

        Si le flux n'est pas disponible du tout — compte sans livestream, API
        en panne — la passerelle ne s'arrête pas : elle se rabat sur une
        relecture périodique plus rapprochée. Mieux vaut un pont qui rafraîchit
        toutes les 5 minutes qu'un conteneur en boucle de redémarrage.
        """
        assert self._client is not None

        async def on_open() -> None:
            LOGGER.info("Flux temps réel établi")
            # Une relecture complète à chaque ouverture rattrape ce qui a pu
            # changer pendant la coupure.
            await self._refresh_all()

        try:
            await self._client.start_event_stream(do_on_livestream_opening_list=[on_open])
        except ApplianceClientException as err:
            self._poll_interval = min(self._poll_interval or 300, 300)
            LOGGER.warning(
                "Flux temps réel indisponible (%s) — repli sur une relecture "
                "toutes les %d s",
                err,
                self._poll_interval,
            )
            await self._stopping.wait()

    async def _refresh_all(self) -> None:
        assert self._client is not None
        for app in self._appliances.values():
            try:
                state = await self._client.get_appliance_state(app.appliance_id)
            except ApplianceClientException as err:
                LOGGER.warning("Relecture impossible pour %s : %s", app.appliance_id, err)
                app.online = False
                self._publish_availability(app)
                self._publish_link(app)
                continue
            self._apply_state(app, state)

    async def _periodic_refresh(self) -> None:
        """Filet de sécurité : relecture complète espacée, au cas où le SSE dérive."""
        while not self._stopping.is_set():
            if self._poll_interval <= 0:
                await self._stopping.wait()
                return
            try:
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=self._poll_interval
                )
                return
            except asyncio.TimeoutError:
                pass
            LOGGER.debug("Relecture périodique")
            await self._refresh_all()

    # ------------------------------------------------------------------
    # Cycle de vie
    # ------------------------------------------------------------------

    async def _make_client(self) -> ApplianceClient:
        api_key, access, refresh = self._load_tokens()
        if not api_key:
            raise SystemExit(
                "Aucune clé API. Renseigner ELECTROLUX_API_KEY (portail "
                "https://developer.electrolux.one/dashboard)."
            )
        if not refresh:
            raise SystemExit("Aucun refresh token. Renseigner ELECTROLUX_REFRESH_TOKEN.")
        if not access:
            LOGGER.info("Pas d'access token : obtention depuis le refresh token")
            access = await self._bootstrap_access_token(refresh)
        self._tokens = TokenManager(
            access_token=access,
            refresh_token=refresh,
            api_key=api_key,
            on_token_update=self._save_tokens,
        )
        return ApplianceClient(self._tokens, external_user_agent=f"electrolux2mqtt/{APP_VERSION}")

    async def setup(self) -> None:
        """Découvre les appareils et construit toutes les entités."""
        assert self._client is not None
        appliances = await self._client.get_appliances()
        if not appliances:
            raise SystemExit(
                "Aucun appareil retourné par l'API : vérifier que l'appareil est "
                "enrôlé dans l'application My Electrolux / AEG, qu'il porte un "
                "nom, et qu'il est connecté au Wi-Fi."
            )
        for appliance in appliances:
            details = await self._client.get_appliance_details(appliance.applianceId)
            state = await self._client.get_appliance_state(appliance.applianceId)
            info = details.applianceInfo
            slug = slugify(appliance.applianceId)
            app = ApplianceBridge(
                appliance_id=appliance.applianceId,
                slug=slug,
                name=appliance.applianceName or info.model or "Electrolux",
                model=info.model or appliance.applianceType,
                brand=info.brand or "Electrolux",
                base_topic=f"{self._topic_prefix}/{slug}",
                availability_topic=f"{self._topic_prefix}/{slug}/availability",
                capabilities=details.capabilities or {},
            )
            app.reported = dict((state.properties or {}).get("reported") or {})
            app.online = (state.connectionState or "").lower() == "connected"
            app.unit = self._resolve_unit(app)
            self._build_entities(app)
            self._appliances[app.appliance_id] = app
            for entity in app.entities.values():
                if entity.build_command is not None:
                    self._commands[app.command_topic(entity)] = (app, entity)
            LOGGER.info(
                "Appareil %s (%s %s, type %s) : %d entité(s), unité %s",
                app.name,
                app.brand,
                app.model,
                appliance.applianceType,
                len(app.entities),
                app.unit,
            )
            if self._read_only:
                LOGGER.info("Mode lecture seule : aucune commande exposée")

    def _resolve_unit(self, app: ApplianceBridge) -> str:
        """Retient une seule unité de température, pour ne pas doubler les entités."""
        if self._unit_pref in {"C", "CELSIUS"}:
            return "CELSIUS"
        if self._unit_pref in {"F", "FAHRENHEIT"}:
            return "FAHRENHEIT"
        reported = app.reported.get("temperatureRepresentation")
        return "FAHRENHEIT" if reported == "FAHRENHEIT" else "CELSIUS"

    async def dump(self) -> None:
        """Mode --dump : recopie appareils, capabilities et état sur stdout."""
        assert self._client is not None
        out: list[dict[str, Any]] = []
        for appliance in await self._client.get_appliances():
            details = await self._client.get_appliance_details(appliance.applianceId)
            state = await self._client.get_appliance_state(appliance.applianceId)
            out.append(
                {
                    "appliance": json.loads(appliance.model_dump_json()),
                    "info": json.loads(details.applianceInfo.model_dump_json()),
                    "capabilities": details.capabilities,
                    "state": json.loads(state.model_dump_json()),
                }
            )
        print(json.dumps(out, indent=2, ensure_ascii=False))

    async def run(self, dump_only: bool = False) -> None:
        self._loop = asyncio.get_running_loop()
        self._client = await self._make_client()
        try:
            if dump_only:
                await self.dump()
                return
            await self.setup()
            self._mqtt = self._connect_mqtt()
            for app in self._appliances.values():
                self._client.add_listener(
                    app.appliance_id,
                    lambda event, app=app: self._on_event(app, event),
                )
            tasks = [
                asyncio.create_task(self._stream()),
                asyncio.create_task(self._periodic_refresh()),
            ]
            stop = asyncio.create_task(self._stopping.wait())
            done, _ = await asyncio.wait(
                [*tasks, stop], return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task is not stop and task.exception() is not None:
                    raise task.exception()  # type: ignore[misc]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """Arrêt propre : appareils marqués hors ligne, sessions fermées."""
        LOGGER.info("Arrêt de la passerelle")
        if self._mqtt is not None:
            for app in self._appliances.values():
                app.online = False
                self._publish_availability(app)
                self._publish_link(app)
            self._mqtt.loop_stop()
            self._mqtt.disconnect()
            self._mqtt = None
        self._client = None

    def request_stop(self) -> None:
        self._stopping.set()


# Variables d'environnement reconnues, et où elles atterrissent dans la
# configuration. Elles priment sur le fichier : c'est ce qui permet de tout
# régler depuis les champs d'un gabarit Unraid, sans éditer de fichier.
ENV_OVERRIDES = {
    "ELECTROLUX_API_KEY": ("electrolux", "api_key"),
    "ELECTROLUX_ACCESS_TOKEN": ("electrolux", "access_token"),
    "ELECTROLUX_REFRESH_TOKEN": ("electrolux", "refresh_token"),
    "ELECTROLUX_REFRESH_INTERVAL": ("electrolux", "refresh_interval"),
    "ELECTROLUX_TEMPERATURE_UNIT": ("electrolux", "temperature_unit"),
    "ELECTROLUX_READ_ONLY": ("electrolux", "read_only"),
    "ELECTROLUX_EXCLUDE": ("electrolux", "exclude"),
    "ELECTROLUX_TOKEN_FILE": ("electrolux", "token_file"),
    "MQTT_HOST": ("mqtt", "host"),
    "MQTT_PORT": ("mqtt", "port"),
    "MQTT_USER": ("mqtt", "login"),
    "MQTT_PASSWORD": ("mqtt", "password"),
    "MQTT_CLIENT_ID": ("mqtt", "client_id"),
    "MQTT_DISCOVERY_PREFIX": ("mqtt", "discovery_prefix"),
    "MQTT_TOPIC_PREFIX": ("mqtt", "topic_prefix"),
    "LOG_LEVEL": ("log", "level"),
}


def load_config(path: str) -> configparser.ConfigParser:
    """Charge le fichier de configuration, puis applique l'environnement.

    Le fichier est facultatif : une configuration entièrement fournie par
    variables d'environnement est valide.
    """
    config = configparser.ConfigParser()
    config.read(path, encoding="utf-8")
    for env_name, (section, option) in ENV_OVERRIDES.items():
        value = os.environ.get(env_name)
        if value is None or value == "":
            continue
        if not config.has_section(section):
            config.add_section(section)
        config.set(section, option, value)
    for section in ("electrolux", "mqtt", "log"):
        if not config.has_section(section):
            config.add_section(section)
    return config


def print_banner(config: configparser.ConfigParser) -> None:
    """Affiche une bannière de démarrage avec un récap de la configuration."""

    def g(section: str, opt: str, default: str = "") -> str:
        return config.get(section, opt, fallback=default)

    lines = [
        f"Version    : {APP_VERSION}",
        f"MQTT       : {g('mqtt', 'host', '127.0.0.1')}:{g('mqtt', 'port', '1883')}"
        f"  (discovery={g('mqtt', 'discovery_prefix', 'homeassistant')},"
        f" topics={g('mqtt', 'topic_prefix', 'electrolux')})",
        f"Electrolux : API officielle, états en push (SSE)"
        f" — filet {g('electrolux', 'refresh_interval', '900')}s",
        "",
        f"Auteur     : {AUTHOR}",
        f"GitHub     : {GITHUB}",
        f"Email      : {EMAIL}",
    ]
    art = [
        r"       _         _           _           ___              _   _   ",
        r"  ___ | | ___  __| |_ _ _ ___| |_  ___ __|_  )_ __  __ _ _| |_| |_ ",
        r" / -_)| |/ -_)/ _|  _| '_/ _ \ | || \ \ / / /| '  \/ _` |  _|  _|  ",
        r" \___||_|\___|\__|\__|_| \___/_|\_,_/_\_\/___|_|_|_\__, |\__|\__|  ",
        r"              Electrolux Group API -> MQTT           |_|           ",
    ]
    width = max(max(len(a) for a in art), max(len(l) for l in lines)) + 2
    out = ["", "+" + "-" * width + "+"]
    for a in art:
        out.append("|" + a.ljust(width) + "|")
    out.append("+" + "-" * width + "+")
    for l in lines:
        out.append("| " + l.ljust(width - 1) + "|")
    out.append("+" + "-" * width + "+")
    out.append("")
    print("\n".join(out), flush=True)


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Passerelle Electrolux Group API vers MQTT Discovery"
    )
    parser.add_argument("config", nargs="?", default=CONFIG_PATH)
    parser.add_argument(
        "--dump",
        action="store_true",
        help="affiche appareils, capabilities et état en JSON, puis quitte",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    logging.basicConfig(
        level=config.get("log", "level", fallback="INFO").upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stderr if args.dump else sys.stdout,
    )
    if not args.dump:
        print_banner(config)
    bridge = Bridge(config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, bridge.request_stop)
        except NotImplementedError:
            # Windows n'a pas add_signal_handler. Le conteneur tourne sous
            # Linux, mais le mode --dump doit rester utilisable partout.
            signal.signal(sig, lambda *_: bridge.request_stop())
    try:
        await bridge.run(dump_only=args.dump)
    except ApplianceClientException as err:
        if getattr(err, "status", None) in (401, 403):
            LOGGER.error(
                "Identifiants refusés par Electrolux (%s) — regénérer la clé API "
                "et les jetons sur https://developer.electrolux.one/dashboard",
                err,
            )
            return 2
        LOGGER.error("Erreur de l'API Electrolux : %s", err)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
