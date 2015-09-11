package cn.sspku.zx.dtm.anno;



public @interface TwoPhrase {
	public String firstAction();
	public String secondAction();
	public String commit();
	public String rollback();
}
