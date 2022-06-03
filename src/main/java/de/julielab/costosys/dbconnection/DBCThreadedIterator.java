package de.julielab.costosys.dbconnection;

import de.julielab.costosys.dbconnection.util.CoStoSysSQLRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 * This iterator class employs multiple Threads for database queries. While the
 * iterator offers access to retrieved values, additional values are
 * concurrently retrieved from the database by another thread.
 * </p>
 * <p>
 * The iterator uses the {@link Exchanger} class to communicate between threads.
 * </p>
 * 
 * @author hellrich/faessler
 * 
 * @param <E>
 */
public abstract class DBCThreadedIterator<E> extends DBCIterator<E> {
	private final static Logger log = LoggerFactory.getLogger(DBCThreadedIterator.class);
	// Exchangers switch results between 2 threads as needed
	protected Exchanger<List<E>> listExchanger = new Exchanger<>();
	protected ConnectionClosable backgroundThread;
	private Iterator<E> currentListIter;
	private boolean hasNext = true;
	private int returned = 0;
	private int currentSize = 0;



	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public E next() {
		E next = currentListIter.next();
		if (!currentListIter.hasNext())
			update();
		return next;
	}

	/**
	 * unsupported
	 */
	public void remove() {
		throw new UnsupportedOperationException();
	}

	protected void update() {
		try {
			log.debug("Retrieving next result list from background thread {}", ((Thread)backgroundThread).getName());
			List<E> list = listExchanger.exchange(null);
			while (list != null && list.isEmpty()) {
				log.debug("Result list was empty; retrieving next result list from background thread {}", ((Thread)backgroundThread).getName());
				list = listExchanger.exchange(null);
			}
			// list full or null
			if (list == null) {
				log.debug("Received end-of-data signal from background thread. Setting 'hasNext' to 'false'.");
				hasNext = false;
			} else {
				log.debug("Received data list of size {} from {}", list.size(), ((Thread)backgroundThread).getName());
				returned = 0;
				currentSize = list.size();
				currentListIter = list.iterator();
			}
		} catch (InterruptedException e) {
			throw new CoStoSysSQLRuntimeException(e);
		}
	}

	@Override
	public abstract void close();


    public abstract void join() throws InterruptedException;
}
