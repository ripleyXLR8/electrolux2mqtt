# Branche `ocp` — mesurer avant de construire

Cette branche n'ajoute **rien** à la passerelle. Elle contient un seul outil de
lecture, `ocp_probe.py`, destiné à répondre à une question factuelle avant
d'écrire la moindre ligne de pont : **l'API interne d'Electrolux (OCP), celle
de l'application mobile, expose-t-elle quelque chose que l'API officielle
n'expose pas ?**

## Pourquoi la question s'est posée

À l'étude initiale, l'OCP avait deux arguments : le temps réel par WebSocket, et
un jeu de propriétés réputé plus large. Le premier est tombé — **l'API
officielle a son propre flux Server-Sent Events**, que `main` utilise déjà. Reste
le second, qui ne se vérifie que sur pièces.

## Ce qui a été testé, et ce que ça coûte

Les jetons du portail développeur **ne franchissent pas** la passerelle OCP :

```
GET https://api.ocp.electrolux.one/appliance/api/v2/appliances
    Authorization: Bearer <jeton du portail développeur>
    x-api-key: <clé du portail>            → 403 {"message":"Forbidden"}
    x-api-key: <clé client de OneApp>      → 403 cas_4402
                                             "Invalid access token type or some
                                              required scope is missing"
```

Ces jetons portent `azp: HeiOpenApi` et le scope `email offline_access` :
l'audience est la bonne, le type de jeton ne l'est pas. L'accès OCP passe par le
**login Gigya du compte** (email + mot de passe), c'est-à-dire par le rejeu du
client d'authentification de l'application mobile, avec un jeton de compte
stocké côté serveur.

C'est exactement la posture de la branche `mobile-api` de `liebherr2mqtt` — mais
là-bas elle se justifiait : la HomeAPI officielle n'exposait **ni l'état de
porte ni les alarmes**. Ici, l'API officielle donne déjà 37 entités sur ce four,
dont la sonde à cœur, le réservoir, la porte et les alertes.

## Utilisation

```sh
pip install pyelectroluxocp
export ELECTROLUX_USERNAME="..." ELECTROLUX_PASSWORD="..."
python tools/ocp_probe.py --official dump.json --out ocp_dump.json
```

`dump.json` est ce que produit `electrolux2mqtt.py --dump`. La sonde aplatit les
deux arbres de capabilities et affiche la différence dans les deux sens.

⚠️ `pyelectroluxocp` est **archivé** en amont, son auteur renvoyant vers le SDK
officiel — ce qui est en soi un argument sur la durabilité de cette voie.

## Décision

**Rien ne bouge sur `main` tant que la sonde n'a pas montré un manque réel.** Si
la colonne « seulement dans l'OCP » revient vide, cette branche a fait son
travail : elle a clos la question au lieu de la laisser traîner.
