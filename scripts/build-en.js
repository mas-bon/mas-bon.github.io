// Genera en/index.html da index.html: stessi contenuti, ma meta/Open Graph in inglese
// e lingua predefinita EN (i crawler social non eseguono JavaScript).
const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');
let html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');

const swap = (from, to) => {
  if (!html.includes(from)) throw new Error('Pattern non trovato: ' + from);
  html = html.split(from).join(to);
};

swap('<html lang="it"', '<html lang="en" data-force-lang="en"');
swap('content="Portfolio di mas-bon - Appassionato di programmazione e web tools. Scopri le mie applicazioni web e progetti open source."',
  'content="mas-bon portfolio - programming and web tools enthusiast. Discover my web apps and open source projects."');
swap('<link rel="canonical" href="https://mas-bon.github.io/" />',
  '<link rel="canonical" href="https://mas-bon.github.io/en/" />');
swap('content="Applicazioni web e progetti open source di mas-bon."',
  'content="Web apps and open source projects by mas-bon."');
swap('<meta property="og:url" content="https://mas-bon.github.io/" />',
  '<meta property="og:url" content="https://mas-bon.github.io/en/" />');

// Percorsi relativi: la pagina vive in /en/
html = html.replace(/(href|src)="\.\/(?!\/)/g, '$1="../');
html = html.replace('href="tailwind.css"', 'href="../tailwind.css"');
html = html.replace("fetch('./apps.json')", "fetch('../apps.json')");

fs.mkdirSync(path.join(root, 'en'), { recursive: true });
fs.writeFileSync(path.join(root, 'en', 'index.html'), html);
console.log('en/index.html generato');
