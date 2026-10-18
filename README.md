# mas-bon.github.io

Portfolio e raccolta di Web App di [mas-bon](https://github.com/mas-bon).

## Progetti

- [Viniloteca](https://mas-bon.github.io/viniloteca/) – gestione della collezione di vinili (PWA)
- [Diario Pressione](https://mas-bon.github.io/diario-pressione/) – diario della pressione arteriosa

Sito statico pubblicato con GitHub Pages: `index.html` è la home, `apps.json` contiene l'elenco delle app (per aggiungerne una basta copiare un oggetto), `viniloteca/` contiene la build di produzione.

Dopo aver modificato le classi Tailwind in `index.html`, rigenera il CSS con `npm install` e `npm run build:css` e committa `tailwind.css` (una GitHub Action verifica che sia aggiornato).

[![Sponsorizza su GitHub](https://img.shields.io/badge/Sponsorizza-GitHub-ea4aaa?style=for-the-badge&logo=github-sponsors&logoColor=white)](https://github.com/sponsors/mas-bon)