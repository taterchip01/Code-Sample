

		//
		// - STEP 1 get the merchants cluster details
		//
		var select = "SELECT * FROM registered_merchants WHERE id=" + cluster + " ALLOW FILTERING";
		console.log(select);
		connection.execute(select,[],function(err, rows){
			try{
				if(rows.length != 0){
					//record found
					cluster = schema_data[0].schema_id;
					cluster_name = schema_data[0].name;
					//set new database cluster
					var dbConfig = {
					 	contactPoints : [cass_version],
							keyspace: cluster
					};
					var connection = new cassandra.Client(dbConfig);
					connection.connect(function(err,result){
						if(err){
							logger.info('API Call: regNewCustomer - FAILED TO CONNECT TO DATABASE;' err + ';' + cluster);
						}
					});
					var schema_data = {};
					//
					// - STEP 2. find the tag and get it's info
					//
					var select = "SELECT * FROM tags WHERE tag_id='" + tag_id + "' ALLOW FILTERING";
					connection.execute(select,[],function(err, rows){
						try{
							if(rows.length != 0){
								//set the tag info
								tag_bal_data = rows['rows'];
								new_reward_pt = parseInt(tag_bal_data[0].reward_points);
								new_balance = parseFloat(tag_bal_data[0].balance);			
								//
								// - STEP 3. look up the parton 
								//
								select = "SELECT * FROM reg_patrons WHERE id='" + patron_id + "' ALLOW FILTERING";
								connection.execute(select,[],function(err, rows){
									try{
										if(rows["rows"].length != 0 ||  rows["rows"].length != null){
											//patronn already has balance
											patron_data = rows['rows'];
											patron_balance = patron_data[0].funds;
											patron_rw_balance = patron_data[0].reward_points;
										}else{
											//parton currently has no balance
											patron_balance = 0;
											patron_rw_balance = 0;
										}
										var updated_fund_balance = new_balance + patron_balance;

										//
										// - Update the registered partons info
										//
										var insert = "INSERT INTO reg_patrons (id,active,date_activated,dl,email,fname,lname,funds,mobile,reward_factor,reward_points,pic_url) VALUES " + 
								  				"('"+ patron_id +"',true,'" + trans_date +"','TBD','NA','"+ patron_fname +"',''," + updated_fund_balance +",'',1,"+new_reward_pt+",'NA');";

										connection.execute(insert,[],function(err){							
											if(err){
												logger.info('API Call: regNewCustomer - UPDATE PATRONS BALANCE ERROR;' err + ';RECORD:' + insert);
												res.status(404).send('Not found');
												return;
											}
										});

										//
										// - UPDATE THE TAG TO REFELECT IT IS NOW REGISTERED
										//
										var update = "UPDATE tags SET is_registered=true,balance=0,reward_points=0, patron_id='"+ patron_id +"' WHERE tag_id='"+ tag_id +"' IF EXISTS";
										connection.execute(update,[],function(err){							
											if(err){
												logger.info('API Call: regNewCustomer - UPDATE TAG FAILED;' err + ';UPDATE:' + insert);
												res.status(404).send('Not found');
												return;
											}
										});

										//
										// - NOW UPDATE THE TAG DATABASE 
										//
										dbConfig = {
											 	contactPoints : [cass_version],
											 	keyspace: "tags"
										};
										connection = new cassandra.Client(dbConfig);
										connection.connect(function(err,result){
											if(err){
												logger.info('API Call: regNewCustomer - FAILED TO CONNECT TO DATABASE;' err + );
												res.status(404).send('Not found');
												return;
											}
										});
										