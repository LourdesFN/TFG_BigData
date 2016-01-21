/*GNU GENERAL PUBLIC LICENSE
      Version 2, June 1991
{TFG-LourdesFernándezNieto}
Copyright (C) {2016}  {Lourdes Fernández Nieto}
*/

var express = require('express');
var router = express.Router();
var stadisticsController = require('../controllers/stadistics_controller');

/* GET home page. */

router.get('/', function(req, res, next) {
  res.render('index');
});

router.get('/idiomas/index', stadisticsController.idiomas);
router.get('/favoritos/index', stadisticsController.favoritos);
router.get('/localizacion/index',stadisticsController.localizacion);
router.get('/seguidores/index', stadisticsController.seguidores);
router.get('/hashtags/index', stadisticsController.hashtags);

router.post('/comenzarAnalisis', stadisticsController.filtrado);
router.post('/finAnalisis', stadisticsController.analisis);

module.exports = router;