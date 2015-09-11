package cn.sspku.zx.dtm.example;

import java.util.Currency;

import cn.sspku.zx.dtm.anno.TwoPhrase;

import com.sun.corba.se.spi.orbutil.fsm.Guard.Result;

@TwoPhrase(firstAction="prepare",secondAction="doTask",
		   commit="commit",rollback="rollback")
public interface DebitAccount {
		
	public Result prepare(Currency cur);
	
	public Result doTask(Currency cur);
	
	public void commit();
	
	public void rollback();
}
