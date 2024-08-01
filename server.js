const express = require('express');
const app = express();
const routes = require('./routes');
const path = require('path');
const porta = 3000;

app.use(express.urlencoded({ extended: true }));

app.use(express.static(path.resolve(__dirname, 'public')));

app.set('views', path.resolve(__dirname, 'src', 'views'));
app.set('view engine', 'ejs');

app.use(routes);

app.listen(porta, () => {
    console.log('Home: http://localhost:3000');
    console.log('Servidor rodando na porta',porta);
});