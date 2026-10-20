# mas-bon.github.io

Portfolio e raccolta di Web App di [mas-bon](https://github.com/mas-bon).

## Progetti

- [Viniloteca](https://mas-bon.github.io/viniloteca/) – gestione della collezione di vinili (PWA)
- [Diario Pressione](https://mas-bon.github.io/diario-pressione/) – diario della pressione arteriosa

Sito statico pubblicato con GitHub Pages: `index.html` è la home, `apps.json` contiene l'elenco delle app (per aggiungerne una basta copiare un oggetto; per la versione inglese aggiungi `description_en`, `category_en`, `badge_en`), il selettore IT/EN in alto traduce la home lato client; `en/index.html` è la versione inglese con Open Graph in inglese (link da condividere: `/en/`) ed è generata da `index.html` con `node scripts/build-en.js` (rigenerala dopo ogni modifica a `index.html`), `viniloteca/` contiene la build di produzione.

Dopo aver modificato le classi Tailwind in `index.html`, rigenera il CSS con `npm install` e `npm run build:css` e committa `tailwind.css` (una GitHub Action verifica che sia aggiornato).

[![Sponsorizza su GitHub](https://img.shields.io/badge/Sponsorizza-GitHub-ea4aaa?style=for-the-badge&logo=github-sponsors&logoColor=white)](https://github.com/sponsors/mas-bon)