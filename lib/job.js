var mongo = require('mongodb');

var Job = module.exports = function Job(kuy,options){
	this.attrs = {
		finishedAt : null,
		status : "pending", //could be pending, ongoing, completed, failed
		locked : false
	};

	//normalize runAt option
	if(  options.runAt instanceof Date ){
		options.runAt = options.runAt.getTime();
	}else if (typeof options.runAt != "number"){
		options.runAt = new Date().getTime();
	}

	this.db = kuy.db;
	this.attrs.jobName = options.jobName;
	this.attrs.data = options.data;
	this.attrs.runAt = options.runAt;
	if(options._id){
		this.attrs._id = options._id;	
	}
	
	this.collectionName = kuy.collectionName;
}


/*
	Saves Job object to mongo db
*/
Job.prototype.save = function(cb){
	var db = this.db;
	var self = this;
	console.log("trying to save job at ",this.collectionName);
	var collection = db.collection(this.collectionName);
	collection.ensureIndex({runAt : 1}, function(errIndex){
		if(errIndex){
			return console.log("error while ensuring index ",errIndex);
		}
		console.log("inserting ",self.attrs);
		collection.insert(self.attrs, function (errInsert, result){
			if(errInsert){
				return console.log("error saving job ", errInsert);
			}
			cb();
		});
	});
}

/*
	finds the earliest job to be executed, then locks it and passes it to cb function
*/
Job.prototype.getFirstJobAndLock = function(cb){
	var self = this;
	var db = this.db;
	var collection = db.collection(this.collectionName);

	collection.findOne({},{sort:{runAt:1}},function(err,result){
		console.log("result ",result);
		result.locked = true;
		collection.update({_id:result._id},{$set:{locked:true}}, function(err,res){
			console.log("Job locked ",res);
			self.attrs = result; //load this job instance with found data
			cb(self);
		});
	});
}

/*
	unlocks this Job Instance
*/
Job.prototype.unlock = function(){
	var db = this.db;
	var collection = db.collection(this.collectionName);
	collection.update({_id:this.attrs._id},{$set:{locked:false}}, function(err,res){
		console.log("Job unlocked ",res);
	});
}

/*
	locks this Job Instance
*/
Job.prototype.lock = function(){
	var db = this.db;
	var collection = db.collection(this.collectionName);
	collection.update({_id:this.attrs._id},{$set:{locked:true}}, function(err,res){
		console.log("Job locked ",res);
	});
}

Job.prototype.work = function(jobfn){
	var self = this;
	jobfn(self.attrs,function(status){

		var db = self.db;
		var collection = db.collection(self.collectionName);
		if(status === true){
			self.attrs.status = "completed";
		}else{
			self.attrs.status = "failed";
		}
		self.attrs.finishedAt = new Date().getTime();
		self.attrs.locked = false;
		console.log("before update ",self.attrs._id);
		collection.update({_id : self.attrs._id}, {$set:self.attrs},function(){
			console.log("job completed");
		});

	});

}
