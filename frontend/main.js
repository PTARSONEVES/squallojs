//import GeraCPF from '../src/modules/GeraCPF';
import 'core-js/stable';
import 'regenerator-runtime/runtime';

import './assets/css/style.css';

(function() {
  const gera = new GeraCPF();
  const cpfGerado = document.querySelector('.cpf-gerado');
  cpfGerado.innerHTML = gera.geraNovoCpf();
})();
