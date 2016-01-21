/*GNU GENERAL PUBLIC LICENSE
      Version 2, June 1991
{TFG-LourdesFernándezNieto}
Copyright (C) {2016}  {Lourdes Fernández Nieto}
*/

exports.idiomas = function(req, res, next) {

  var fs = require("fs");
  var filename = '/TFG/ResultadoAnalisis/Idiomas/part-00000';
  var s = fs.readFileSync(filename).toString();
  var s2 = ('[' + s.replace(/\n/g, "").replace(/,$/, "") +']');
  var datos = JSON.parse(s2);
  var datosParaPintar = datos.map(function(r){
    return +r.cantidad || 0;
  });
  var datosEtiquetas  = datos.map(function(r){
    return r.idioma;
  });
  console.log(datosParaPintar);
  console.log(datosEtiquetas);

  res.render('idiomas/graficaIdiomas',{ d:datosParaPintar, e:datosEtiquetas });

};

exports.favoritos = function(req, res, next) {
  
  var fs = require("fs");
  var filename = '/TFG/ResultadoAnalisis/InfluenciaFavoritos/part-00000';
  var s = fs.readFileSync(filename).toString();
  var s2 = ('[' + s.replace(/\n/g, "").replace(/,$/, "") +']');
  var datos = JSON.parse(s2);
  var datosParaPintar = datos.map(function(r){
    return +r.numero || 0;
  });
  var datosEtiquetas  = datos.map(function(r){
    return r.identificador;
  });
  console.log(datosParaPintar);
  console.log(datosEtiquetas);

  res.render('favoritos/graficaFavoritos',{ datay:datosParaPintar, datax:datosEtiquetas });
 
};

exports.localizacion = function(req, res, next) {

  var fs = require("fs");
  var filename = '/TFG/ResultadoAnalisis/Lugares/part-00000';
  var s = fs.readFileSync(filename).toString();
  var s2 = ('[' + s.replace(/\n/g, "").replace(/,$/, "") +']');
  var datos = JSON.parse(s2);
  var datosParaPintar = datos.map(function(r){
    return +r.cantidad || 0;
  });
  var datosEtiquetas  = datos.map(function(r){
    return r.lugar;
  });
  console.log(datosParaPintar);
  console.log(datosEtiquetas);

  res.render('localizacion/graficaLocalizacion',{ datay:datosParaPintar, datax:datosEtiquetas });
  
};

exports.seguidores = function(req, res, next) {

  var fs = require("fs");
  var filename = '/TFG/ResultadoAnalisis/InfluenciaSeguidores/part-00000';
  var s = fs.readFileSync(filename).toString();
  var s2 = ('[' + s.replace(/\n/g, "").replace(/,$/, "") +']');
  var datos = JSON.parse(s2);
  var datosParaPintar = datos.map(function(r){
    return +r.numero || 0;
  });
  var datosEtiquetas  = datos.map(function(r){
    return r.identificador;
  });
  console.log(datosParaPintar);
  console.log(datosEtiquetas);

  res.render('seguidores/graficaSeguidores.ejs',{ datay:datosParaPintar, datax:datosEtiquetas });
  
};
exports.hashtags = function(req, res, next) {

  var fs = require("fs");
  var filename = '/TFG/ResultadoAnalisis/Hashtags/part-00000';
  var s = fs.readFileSync(filename).toString();
  var s2 = ('[' + s.replace(/\n/g, "").replace(/,$/, "") +']');
  var datos = JSON.parse(s2);
  var datosParaPintar = datos.map(function(r){
    return +r.cantidad || 0;
  });
  var datosEtiquetas  = datos.map(function(r){
    return r.hashtag;
  });
  console.log(datosParaPintar);
  console.log(datosEtiquetas);

  res.render('hashtags/graficaHashtags.ejs',{ datay:datosParaPintar, datax:datosEtiquetas });

};

//-----------------------------------------------------------------------//
//-----------------------------------------------------------------------//
//---LLAMADA A LAS CLASES EN SCALA QUE DESCARGAN Y ANALIZAN LOS TWEETS---//
//-----------------------------------------------------------------------//
//-----------------------------------------------------------------------//

var aux = null; //Variable que nos sirve para realizar la parada 

exports.filtrado = function(req, res, next) {

  var spawn = require('child_process').spawn,
      borrarDirectorio1 = spawn('rm',['-rf', '/TFG/BaseDatosTweets']),
      borrarDirectorio2 = spawn('rm',['-rf', '/TFG/ResultadoAnalisis']);

  var filtro = req.body.etiqueta;
  var spawn = require('child_process').spawn,
      comandoBD  = spawn('/spark/spark-1.4.1-bin-hadoop2.4/bin/spark-submit', ['--class', 'BaseDatos', '/TFG/BigData/AlmacenTweets/target/scala-2.10/acceso-twitter-assembly-1.0.jar', '/TFG/BaseDatosTweets', filtro]);

  comandoBD.stdout.on('data', function (data) {
      console.log('stdout: ' + data);
    });
  comandoBD.stderr.on('data', function (data) {
    console.log('stderr: ' + data);
  });
  comandoBD.on('close', function (code) {
    console.log('child process exited with code ' + code);
  });
  aux = comandoBD.pid;

  res.render('comenzarAnalisis/index.ejs',{});

 
};
exports.analisis= function(req, res, next) {

  console.log('El PID de la variable auxiliar es ' + aux);

  if(aux != null){

    process.kill(aux);
    aux = null;
    var spawn = require('child_process').spawn,
        comandoAnalisisTweets  = spawn('/spark/spark-1.4.1-bin-hadoop2.4/bin/spark-submit', ['--class', 'AnalisisTweets', '/TFG/BigData/ClasificacionTweets/target/scala-2.11/analisis-tweets-assembly-1.0.jar', '/TFG/BaseDatosTweets/index.txt', '/TFG/ResultadoAnalisis']);

    comandoAnalisisTweets.stdout.on('data', function (data) {
      console.log('stdout: ' + data);
    });
    comandoAnalisisTweets.stderr.on('data', function (data) {
      console.log('stderr: ' + data);
    });
    comandoAnalisisTweets.on('close', function (code) {
      console.log('child process exited with code ' + code);
    });
  }

  res.render('finAnalisis/index.ejs',{});

};
