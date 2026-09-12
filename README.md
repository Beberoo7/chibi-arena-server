# Chibi Fighter Ultimate V69 — Backend Render

Cette version conserve **Arena 1v1** et ajoute le **Battleground Online 2 à 8 joueurs humains**.

## Déploiement Render
Remplace les fichiers du dépôt `chibi-arena-server` par ceux de ce dossier puis commit/push.
Le `render.yaml` garde :
- Build Command : `npm install`
- Start Command : `npm start`
- Health Check : `/health`

## Protocoles
Arena existante :
`create_room`, `join_room`, `resume`, `start_match`, `submit_plans`, `forfeit_match`.

Battleground V69 :
`bg_create_room`, `bg_join_room`, `bg_resume`, `bg_start`, `bg_assignment_ack`,
`bg_submit_plans`, `bg_round_ack`, `bg_redistribution_choice`,
`bg_redistribution_ack`, `bg_forfeit_match`, `bg_leave_room`.

## Battleground
- 2 à 8 joueurs humains, le reste en bots.
- 8 équipes de 2 Chibis.
- Attribution aléatoire serveur.
- Pas de Tank+Tank ni Support+Support.
- Aucune paire identique au lancement.
- Matchmaking serveur, PV Battleground, éliminations et spectres.
- Redistribution facultative avant le round 7.
- Reconnexion 90 s ; après expiration, l'équipe passe sous contrôle bot.
