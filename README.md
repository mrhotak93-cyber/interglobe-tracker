# InterGlobe Tracker Web MVP

Web app locale MVP pour **InterGlobe Tracker** avec 3 rôles :
- **Dispatch**
- **Chauffeur**
- **Client**

## Fonctions incluses
- Connexion **nom d'utilisateur / mot de passe**
- Attribution des rôles par le dispatch
- Création d'utilisateurs
- Création de tournées
- Ajout / modification / suppression d'arrêts
- Duplication d'un planning d'un jour à un autre
- Création de plannings récurrents
- Génération des tournées récurrentes
- Vue chauffeur avec boutons :
  - Commencer la tournée
  - Ouvrir Waze
  - Arrivé
  - Départ
  - Upload des preuves photo
  - Terminer la tournée
- Suivi de localisation pendant que la page chauffeur reste ouverte
- Vue client avec :
  - état des arrêts
  - heures d'arrivée / départ
  - preuves photo
  - dernier point GPS
  - mini rapport de fin de tournée

## Limite importante du web
Le tracking GPS fonctionne **tant que la page chauffeur reste ouverte et autorisée à utiliser la position**.  
Pour un vrai tracking arrière-plan robuste, une app Android reste préférable.

## Installation
1. Installe Node.js
2. Ouvre un terminal dans ce dossier
3. Lance :

```bash
npm install
npm start
```

4. Ouvre :
```txt
http://localhost:3000
```

## Comptes de démo
- Dispatch : `dispatch` / `1234`
- Chauffeur : `driver` / `1234`
- Client : `client` / `1234`

## Structure
- `server.js` : backend Express + logique métier
- `public/` : CSS et uploads
- `views/` : templates EJS
- `data/interglobe.db` : base SQLite créée automatiquement

## Notes
- Les mots de passe sont hashés avec bcrypt.
- Les photos sont stockées dans `public/uploads`.
- Les kilomètres sont estimés depuis les points GPS enregistrés.
- Les tournées générées depuis une récurrence restent modifiables une par une.
