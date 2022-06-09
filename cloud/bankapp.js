Parse.Cloud.beforeSave("BankAccount", async request => {
    const action = request.object.get("action");
    const amount = request.object.get("amount");
    const accountNum = request.object.get("accountNum");
    const params =  { "accountNum": accountNum };
    const balance = await Parse.Cloud.run("balance",params);
    // Verify current balance > withdrawal amount
    if (action === "Withdrawal" && amount > balance ) {
        throw "Withdrawal Amount is greater than current balance of " + balance;
    }
    // Make Withdrawals negative, Balance is just summed
    if (action === "Withdrawal") {
      request.object.set("amount", amount * -1)
    }
    },{
      fields: {
        accountNum : {
          required:true,
        },
        action : {
          required:true,
          options: action => {
            return action === "Deposit" || action === "Withdrawal";
          },
          error: 'action must be either a Deposit or Withdrawal'
        },
        amount : {
          required:true,
          options: amount => {
            return amount > 0;
          },
          error: 'amount must be greater than 0'
        }
      }
    });

    Parse.Cloud.define('balance', async (request) => {
      console.log("CDD In balance accountNum = %s",request.params.accountNum);
      const query = new Parse.Query("BankAccount");
      query.equalTo("accountNum", request.params.accountNum);
      const results = await query.find({ useMasterKey: true });
      let sum = 0;
      for (let i = 0; i < results.length; ++i) {
        sum += results[i].get("amount");
      }
      console.log("CDD In balance sum = %s",sum);
      return sum;
    });

    Parse.Cloud.define('getTransactionHistory', async req => {
        req.log.info(req);
        const query = new Parse.Query("BankAccount");
        query.equalTo("accountNum", req.params.accountNum);
        const result = await query.find({ useMasterKey: true });
        return result
      });