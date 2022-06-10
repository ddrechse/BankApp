initialized = false

Parse.Cloud.beforeSave("BankAccount", async request => {
    const action = request.object.get("action");
    const amount = request.object.get("amount");
    const accountNum = request.object.get("accountNum");
    const toAccountNum = request.object.get("toAccountNum")
    const externalAccountNum = request.object.get("externalAccountNum")
    const fromAccountNum = request.object.get("fromAccountNum")
    const balance = await Parse.Cloud.run("balance",{ "accountNum": accountNum });

    // Verify current balance > withdrawal amount
    if (action === "Withdrawal" && amount > balance ) {
        throw "Withdrawal Amount is greater than current balance of " + balance;
    }

    // Verify toAccountNum or externalAccountNum exists when action = Transfer
    if(action === "Transfer" && (typeof toAccountNum === 'undefined' && typeof externalAccountNum === 'undefined' && typeof fromAccountNum === 'undefined'))  {
        throw "Missing toAccountNum(internal), externalAccountNum(External) or fromAccountNum(internal) for Transfer Action";
    }
    if(action === "Transfer" && (typeof toAccountNum !== 'undefined' && typeof externalAccountNum !== 'undefined'))  {
        throw "Both toAccountNum(internal) and externalAccountNum(External) for Transfer is invalid.   Pick one or the other";
    }

    // Verify local transfer account exists
    if(typeof toAccountNum !== 'undefined') {
        const accountExists = await Parse.Cloud.run("accountexists", { "accountNum": toAccountNum });
        if(action === "Transfer" && !accountExists){
            throw ("Transfer toAccountNum does not exist, toAccountNum = %s", toAccountNum);
        }
            
        // Verify current balance > transfer amount
        if (action === "Transfer" && amount > balance ) {
            throw "Transfer Amount is greater than current balance of " + balance;
        }
    }

    // Verify current balance > transfer amount
    if (action === "Transfer" && amount > balance ) {
        throw "Transfer Amount is greater than current balance of " + balance;
    }
    // Make Withdrawals negative, Balance is just summed
    if (action === "Withdrawal" || action === "Transfer" && typeof fromAccountNum === 'undefined') {
      request.object.set("amount", amount * -1)
    }
    },{
      fields: {
        accountNum : {
          required:true,
          options: accountNum => {
            return accountNum > 0;
          },
          error: 'accountNum must be greater than 0'          
        },
        action : {
          required:true,
          options: action => {
            return action === "Deposit" || action === "Withdrawal" || action === "Transfer";
          },
          error: 'action must be either a Deposit, Withdrawal or Transfer'
        },
        amount : {
          required:true,
          options: amount => {
            return amount > 0;
          },
          error: 'amount must be greater than 0'
        },
        toAccountNum : {
            required:false,
            options: toAccountNum => {
              return toAccountNum > 0;
            },
            error: 'toAccountNum must be greater than 0'          
        },     
        externalAccountNum : {
            required:false,
            options: externalAccountNum => {
              return externalAccountNum > 0;
            },
            error: 'externalAccountNum must be greater than 0'          
        }            
      }
    });

Parse.Cloud.define('balance', async (request) => {
      const query = new Parse.Query("BankAccount");
      query.equalTo("accountNum", request.params.accountNum);
      const results = await query.find({ useMasterKey: true });
      let sum = 0;
      for (let i = 0; i < results.length; ++i) {
        sum += results[i].get("amount");
      }
      return sum;
    });

Parse.Cloud.define('accountexists', async (request) => {
        const query = new Parse.Query("BankAccount");
        query.equalTo("accountNum", request.params.accountNum);
        const results = await query.find({ useMasterKey: true });
        var count = Object. keys(results).length
        return count > 0;
      });

Parse.Cloud.define('history', async req => {
        req.log.info(req);
        const query = new Parse.Query("BankAccount");
        query.equalTo("accountNum", req.params.accountNum);
        const result = await query.find({ useMasterKey: true });
        return result
      });

Parse.Cloud.afterSave("BankAccount", async (request) => {
        const action = request.object.get("action");
        const toAccountNum = request.object.get("toAccountNum")
        const externalAccountNum = request.object.get("externalAccountNum")


        if (action === "Transfer" && typeof externalAccountNum !== 'undefined') {
            amount =  request.object.get("amount");
            let message_content = "{\"accountNum\":"+externalAccountNum+ ", \"action\":\"Deposit\", \"amount\":"+amount*-1+"}";
            result = await publish(message_content);
        }

        if (action === "Transfer" && typeof toAccountNum !== 'undefined') {
            const BankAccount = Parse.Object.extend("BankAccount");
            const bankaccount = new BankAccount();

            bankaccount.set("accountNum", toAccountNum);
            const fromAccountNum = request.object.get("accountNum");
            bankaccount.set("fromAccountNum", fromAccountNum);
            bankaccount.set("action", "Transfer");
            const amount = request.object.get("amount");
            bankaccount.set("amount", amount*-1);
            console.log(bankaccount);

            let result;
            await bankaccount.save()
            .then((bankaccount) => {
              // Execute any logic that should take place after the object is saved.
              result = 'New object created with objectId: ' + bankaccount.id;
            }, (error) => {
              // Execute any logic that should take place if the save fails.
              // error is a Parse.Error with an error code and message.
              result = 'Failed to process Transfer, with error code: ' + error.message;
            });
            
            return result;
        }
      });

    async function publish(message_content) {
        const oracledb = require('oracledb')
        const tnsnames = "/wallet"
    
    
        oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
        if(initialized === false) {
            oracledb.initOracleClient({configDir: tnsnames});
            
            await runSQL(`
            declare
                already_exists number;
            begin 
                select count(*)
                into already_exists
                from user_queues
                where upper(name) = 'PUBLISH';
                if already_exists = 0 then  
                    DBMS_AQADM.CREATE_QUEUE_TABLE (
                    queue_table => 'publish_queuetable',
                    queue_payload_type  => 'RAW');   
                    DBMS_AQADM.CREATE_QUEUE ( 
                    queue_name => 'publish',
                    queue_table => 'publish_queuetable');  
                    DBMS_AQADM.START_QUEUE ( 
                    queue_name => 'publish');
                end if;
            end;`)
    
    
        initialized = true;
        }
    
        await runSQL(`
        DECLARE
            enqueue_options     dbms_aq.enqueue_options_t;
            message_properties  dbms_aq.message_properties_t;
            message_handle      RAW(16);
            message             RAW(4096); 
        
        BEGIN
            message :=  utl_raw.cast_to_raw('` + message_content + `'); 
            DBMS_AQ.ENQUEUE(
                queue_name           => 'publish',           
                enqueue_options      => enqueue_options,       
                message_properties   => message_properties,     
                payload              => message,               
                msgid                => message_handle);
            COMMIT;
        END;`)
    }

    async function runSQL(statement) {
        const oracledb = require('oracledb')
    
        const user = "user";
        const password = "password";
        const connectString = "database";
        let connection;
        try {
            connection = await oracledb.getConnection({
                user: user,
                password: password,
                connectString: connectString
            });
            const result = await connection.execute(statement);
            console.log(result.rows);
            return result.rows
        } catch (err) {
            console.log(err);
        } finally {
            if (connection) {
                try {
                    await connection.close();
                } catch (err) {
                    console.log(err);
                }
            }
        }
      }
