var Kuy = require('./index.js');
var kuy = new Kuy({mongo_url : "",
	success : function(){
		kuy.defineJob('testJob',function(job,done){
			console.log("job received with ",job);
			done();
		});

		kuy.schedule(new Date().getTime(),'testJob',{"someData":"someData"});
		
		kuy.start();
	}
});

