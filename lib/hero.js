var express = require('express')      // basic framework
, mongodb = require("mongodb")      // document data store
, redis = require("redis")        // key-value data store
, extend  = require('xtend')      // Merge object properties
, request = require('request')    // Request library
,   amqp    = require('amqp')     // AMQP implementation for rabbit
,   qlog  = require('qlog-node')
, app   = express()
, path  = require('path')
, HeroIODocs  = require('./hero_iodocs.js').HeroIODocs
;

var hero = this;

hero.dbType = {
 MONGODB : 'mongodb'
 ,  
 REDIS  : 'redis'
};

hero.mqType = {
 RABBITMQ : 'rabbitmq'
 ,  
 AMQP    : 'amqp'
};

var config = null;

function getParamValue (p_label) {
 var idx=-1;
 for ( var f=0, F=process.argv.length; f<F; f++ ) {
  idx = process.argv[f].indexOf( p_label+'=' )
  if ( idx !== -1 ){
   return process.argv[f].substring( idx + String(p_label+'=').length );
  }
 }
 return null;
}

function error (){
 console.log('* * * * E R R O R * * * *', arguments);
}

function log() {
 console.log('<---- [ LOG ] ---->', arguments);
}

function db(p_config){
 var config = p_config;
 var self = this;

 function reset (f_callback){
  switch(config.type){

   case hero.dbType.MONGODB :       
    self.client.dropDatabase(f_callback);
    break;

   case hero.dbType.REDIS :
    self.client.flushdb(f_callback);
    break;

  }
 }

 function connection(f_callback) {
  if ( self.client ) {
   f_callback( null, self.client );
  }
  else {
   switch(config.type){
    case hero.dbType.MONGODB :
     if (p_config.uri) {
      mongodb.Db.connect( 
       p_config.uri
       , 
       p_config.params
       , 
       function(err, client) {
        if(err) { 
         hero.error(err);;
        }
        self.client = client;
        f_callback( err, self.client );
       }
       );
     } 
     else {
      self.client = new mongodb.Db(
       p_config.name
       ,  
       new mongodb.Server(p_config.host, p_config.port)
       ,  
       p_config.params
       );

      self.client.open(
       function(err, p_client) {
        if(err) {
         hero.error(err);
        }
        f_callback( err, self.client );
       }
       );
     }
     break;
    case hero.dbType.REDIS :
     self.client = redis.createClient(p_config.port, p_config.host, p_config.params);
     f_callback(null, self.client);
     break;
    default:
     hero.error('database "'+config.type+'" is not supported');
     break;
   }
  }
 }

 function setup(f_callback){
  connection( f_callback );
 }

 self.client = null;
 self.setup  = setup;
 self.reset  = reset;

}

function mq(p_config){
 var defaults =   {
  "host"      : "localhost"
  ,   
  "port"      : "5672"
  ,   
  "exchange"  : ""
  ,   
  "exchange_opts"   : {}
  , 
  "routing_key" : "*"
  , 
  "routing_opts" : {}
  ,   
  "queue"     : "defQue"
  ,   
  "queue_opts"   : {
   "type" : "topic"
  }
  ,   
  "type"      : "amqp"
 };

 var _config  = extend(defaults,p_config);
 var auth = (_config.user || "");
 if(auth.length > 0) {
  if(_config.password) {
   auth += ":"+_config.password + "@";
  } else {
   auth += "@";
  }
 }
 _config.url = (_config.url ? _config.url : 'amqp://' + auth + _config.host + (!!_config.port && _config.port.length > 0 ? ':' + _config.port : ''));
  
 var  _mqConn   = null
 ,  _exchange   = null
 ,  _queue    = null
 ;

 function _connection(f_callback) {

  switch(_config.type){

   case hero.mqType.AMQP :

    if ( _mqConn === null ) {
     _mqConn = amqp.createConnection( {
      url : _config.url
     } );
     _mqConn.on(
      'ready'
      , 
      function(){
       f_callback();
      }
      );
    }
    else {
     f_callback();
    }
    break;

   default :
    hero.error('mq "'+config.type+'" is not supported');
    break;

  }

 }

 function _on(f_callback) {
  _connection( 
   function () {
    if(_exchange === null && _config.exchange.length > 0) {
     _exchange = _mqConn.exchange(_config.exchange, _config.exchange_opts);
    }

    if( _queue === null ){
     _mqConn.queue(
      _config.queue
      , 
      _config.queue_opts
      ,
      function(q){
       _queue = q;
       _config.exchange.length > 0 ? q.bind(_config.exchange, _config.routing_key) : q.bind(_config.routing_key);
       q.subscribe(f_callback);
      }
      );
    }
   }
   );
 }

 function _notify(p_data) {
  _route(_config.routing_key, p_data);
 }

 function _route(p_key, p_data) {
  _connection(
   function () {
    if( !_exchange ){
     _exchange = _mqConn.exchange( _config.exchange, _config.exchange_opts );
    }
    _exchange.publish(p_key, p_data, _config.routing_opts);
   }
   );
 }

 function _reset() {
  if (_mqConn){
   _mqConn.end();
   _queue     = null;
   _exchange  = null;
   _mqConn  = null;
  }
 }

 this.on  = _on;
 this.notify = _notify;
 this.route   = _route;
 this.reset   = _reset;

}

hero.worker = function (f_class){
 var self = {};
  
 self.config = config;
 self.error  = error;

 var dbs = {};
 self.db = function ( p_label, p_config ){
  if ( !dbs[p_label] || arguments.length === 2 ) {
   dbs[p_label] = new db( p_config );
  }
  return dbs[p_label];
 };

 var mqs = {};
 self.mq = function ( p_label, p_config ){
  if ( !mqs[p_label] && arguments.length === 2 ) {
   mqs[p_label] = new mq( p_config );
  }
  return mqs[p_label];
 };

 f_class(self);

 return self;
}

hero.service = function (f_class){
 var self = {};
  
 self.config = config;
 self.error  = error;

 return f_class(self);
}

function registerPath(p_path, p_method, f_handler){
 console.log("REGISTER", p_method, p_path);
 switch(p_method) {
  case "GET":
   app.get(p_path, f_handler);
   break;
  case "POST":
   app.post(p_path, f_handler);
   break;
  case "PUT":
   app.put(p_path, f_handler);
   break;
  case "OPTIONS":
   app.options(p_path, f_handler);
   break;
  case "DELETE":
   app.delete(p_path, f_handler);
   break; 
 }
}

hero.error = error;
hero.log = log;
hero.config = function (){
 return config;
};

hero.init = function (iodocsApisConfig, basedir, f_callback){
 if (config.iodocs.active) {
  new HeroIODocs(basedir, config.iodocs.dir, iodocsApisConfig);
 }

 hero.urls = {
  "GET": {},
  "POST": {},
  "PUT": {},
  "DELETE": {}
 }

 for (var apiName in iodocsApisConfig) {
  var api = require(path.join(basedir, config.iodocs.dir, "public/data", apiName + ".json"))
  for (var i = 0; i < api.endpoints.length; i++) {
   var endpoint = api.endpoints[i];
   for (var j = 0; j < endpoint.methods.length; j++) {
    var method = endpoint.methods[j];

    hero.urls[method.HTTPMethod][method.URI] = {
     required: 0
    };
        
    if (Object.prototype.toString.call(method.hero.handler) !== '[object Array]') {
     method.hero.handler = [method.hero.handler];
    }
    
    var requires = [];
    if (config.iodocs.active) {
      requires.push(hero.prune);
    }
    var k = 0;
    for (var k = 0; k < method.hero.handler.length; k++) {
     requires.push(require(path.join(basedir, 'src', apiName, "path", method.hero.handler[k])));  
    }

    for (k = 0; k < method.parameters.length; k++) {
     var param = method.parameters[k];
     var name = param.Name;
     if (param.Location === "header") {
      name = name.toLowerCase();  
     }
     hero.urls[method.HTTPMethod][method.URI][name] = {
      location: param.Location, 
      required: param.Required === "Y" || param.Required === true
      };
     if (param.Required === "Y" || param.Required === true) {
      hero.urls[method.HTTPMethod][method.URI].required = hero.urls[method.HTTPMethod][method.URI].required + 1;
     }
    }
       
    registerPath(method.URI, method.HTTPMethod, requires);
        
   }
  }
 }
 if (config.iodocs.active) {
  var iodocs  = require('iodocs').iodocs(path.join(basedir, config.iodocs.dir));
 }

 f_callback();
};

hero.getProcParam = getParamValue;

var paramPort = getParamValue('port');
var paramEnv  = getParamValue('env');

if ( paramEnv === null || paramEnv === '') {
 hero.error('"env" initial parameter is not found, it must specify some correct value');
}
else {
 config = require(process.cwd() + '/lib/config/'+paramEnv+'.json');
 if ( config === null ) {
  hero.error('environment '+paramEnv+' not found');
 }
}

if (config.qlog) {
 qlog.config(config.qlog, function(err){
  if ( !err ){
   hero.error = function(message, tags) {
    error(arguments);
    qlog.notify(message, 'error,' + tags);
   };

   hero.log = function(message, tags){
    log(arguments);
    qlog.notify(message, tags);
   }
  } else {
   hero.error("QLog service not available", err);
  }
 });
}

if ( paramPort === null || paramPort === '') {
 hero.error('"port" initial parameter is not found, it must specify some correct value');
}

hero.env = function (){
 return paramEnv;
}

hero.port = function (){
 return paramPort;
}

function prune (defParams, req, location, iodocsLocation) {
 var required = 0;
 iodocsLocation = iodocsLocation ? iodocsLocation : location
 for (param in req[location]) {
  if (location === "headers") {
    param = param.toLowerCase();
  }
  var msg = "";
  if (!defParams[param]) {
   msg = "The " + param + " param is not defined and it will be pruned. URL: " + req.route.path + " Method " + req.method;
  } else if (defParams[param].location !== iodocsLocation) {
   msg = "The " + param + " param is not a " + iodocsLocation + " param and it will be pruned. URL: " + req.route.path + " Method " + req.method;
  } else if ( defParams[param]["required"] ) {
   required++;
  }
  if (msg && location !== "headers") {
   console.log(msg);
   delete req[location][param];
  }
 }
 return required;
}

hero.prune = function (req, res, next) {
 var defParams =  hero.urls[req.method][req.route.path];
 var required = 0;
  
 required += prune(defParams, req, "body");
 required += prune(defParams, req, "query");
 required += prune(defParams, req, "params", "pathReplace");
 required += prune(defParams, req, "headers", "header");


 if (defParams.required > required) {
  console.log("There are " + defParams.required + " required params but only " + required + " have been passed. URL: " + req.route.path + " Method " + req.method);
 }
 next();
};

console.log('getting starting parameters -> environment['+paramEnv+'] port['+paramPort+']');

hero.app = app;
module.exports = hero;