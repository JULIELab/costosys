package de.julielab.xmlData.dataBase;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Exchanger;

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
	// Exchangers switch results between 2 threads as needed
	protected Exchanger<List<E>> listExchanger = new Exchanger<List<E>>();
	protected Iterator<E> currentListIter;
	protected boolean hasNext = true;
	protected Thread backgroundThread;

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
			List<E> list = listExchanger.exchange(null);
			while (list != null && list.isEmpty()) {
				list = listExchanger.exchange(null);
			}
			// list full or null
			if (list == null) {
				hasNext = false;
			} else
				currentListIter = list.iterator();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see de.julielab.xmlData.dataBase.DBCIterator#destroy()
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	
}
