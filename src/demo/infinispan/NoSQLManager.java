package demo.infinispan;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.EntryMetaFactory;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;

public class NoSQLManager<T, Z extends Object> {

	private Configuration configuration;
	private DefaultCacheManager manager;
	private Cache<T, Z> cache;
	private TransactionManager transactionManager;

	public NoSQLManager() {
		this.manager = new DefaultCacheManager();
		this.configuration = new ConfigurationBuilder().transaction().lockingMode(LockingMode.OPTIMISTIC).transactionMode(TransactionMode.TRANSACTIONAL).build();
		String newCacheName = "repl";
		manager.defineConfiguration(newCacheName, configuration);
		this.cache = manager.getCache(newCacheName);
		this.transactionManager = cache.getAdvancedCache().getTransactionManager();
	}
	
	public TransactionManager getTransaction() {
			return this.transactionManager;		
	}
	
	public Cache<T, Z> getCache(){
		return this.cache;
	}
	
	public void save(Map<T, Z> records){
		try {
			transactionManager.begin();
			for (Entry<T, Z> entry : records.entrySet()) {
				cache.putIfAbsentAsync(entry.getKey(), entry.getValue());
			}
			transactionManager.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void saveButRollback(Map<T, Z> records){
		try {
			transactionManager.begin();
			for (Entry<T, Z> entry : records.entrySet()) {
				cache.putIfAbsentAsync(entry.getKey(), entry.getValue());
			}
			transactionManager.rollback();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public int count(){
		return cache.keySet().size();
	}

}
