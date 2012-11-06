/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.search;

import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.middleware.types.MetaClass;
import Sirius.server.middleware.types.MetaObject;

import org.apache.log4j.Logger;

import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import de.cismet.cids.custom.sudplan.commons.SudplanConcurrency;

import de.cismet.cids.server.search.AbstractCidsServerSearch;

/**
 * DOCUMENT ME!
 *
 * @author   martin.scholl@cismet.de
 * @version  $Revision$, $Date$
 */
public final class UnfinishedRunSearchStatement extends AbstractCidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    private static final String STMT_SEARCH_UNFINISHED_RUNS =
        "SELECT id FROM run WHERE finished IS NULL AND started IS NOT NULL"; // NOI18N
    private static final String STMT_TEST_SUDPLAN_SYSTEM = "SELECT r.id, mi.id, mo.id, m.id "
                + "FROM run r, modelinput mi, modeloutput mo, model m "
                + "LIMIT 1";

    private static final transient Logger LOG = Logger.getLogger(UnfinishedRunSearchStatement.class);

    //~ Methods ----------------------------------------------------------------

    @Override
    public Collection performServerSearch() {
        final ExecutorService searcher = Executors.newCachedThreadPool(
                SudplanConcurrency.createThreadFactory("unfinished-run-search")); // NOI18N

        final Map map = getActiveLocalServers();
        final ArrayList<RunFetcher> fetchers = new ArrayList<RunFetcher>(map.size());
        for (final Object o : map.keySet()) {
            final String domain = (String)o;
            final MetaService ms = (MetaService)map.get(domain);

            final RunFetcher fetcher = new RunFetcher(ms, domain);
            // keep track of the fetchers since we want to know the results afterwards
            fetchers.add(fetcher);
            // we don't need the future since we'return going to shutdown the service anyway
            searcher.submit(fetcher);
        }

        searcher.shutdown();
        // we wait for the searcher to shutdown and bail out returning null if there is any abnormal termination
        try {
            if (!searcher.awaitTermination(2, TimeUnit.MINUTES)) {
                LOG.error("the searches did not finish within 2 minutes, search unsuccessful, returning null"); // NOI18N

                return null;
            }
        } catch (final InterruptedException ex) {
            LOG.error("waiting for the searcher pool was interrupted, search unsuccessful, returning null"); // NOI18N

            return null;
        }

        final ArrayList<MetaObject> unfinished = new ArrayList<MetaObject>();
        for (final RunFetcher fetcher : fetchers) {
            if (fetcher.getException() == null) {
                unfinished.addAll(fetcher.getResult());
            } else {
                LOG.error(
                    "at least one RunFetcher terminated abnormally, search unsuccessful, returning", // NOI18N
                    fetcher.getException());

                return null;
            }
        }

        return unfinished;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @version  $Revision$, $Date$
     */
    private final class RunFetcher implements Runnable {

        //~ Instance fields ----------------------------------------------------

        private final transient MetaService ms;
        private final transient String domain;

        private final transient List<MetaObject> result;
        private transient Exception exception;

        //~ Constructors -------------------------------------------------------

        /**
         * Creates a new RunFetcher object.
         *
         * @param  ms      DOCUMENT ME!
         * @param  domain  DOCUMENT ME!
         */
        RunFetcher(final MetaService ms, final String domain) {
            this.ms = ms;
            this.domain = domain;
            this.exception = null;
            this.result = new ArrayList<MetaObject>();
        }

        //~ Methods ------------------------------------------------------------

        /**
         * DOCUMENT ME!
         *
         * @return  DOCUMENT ME!
         */
        List<MetaObject> getResult() {
            return result;
        }

        /**
         * DOCUMENT ME!
         *
         * @return  DOCUMENT ME!
         */
        Exception getException() {
            return exception;
        }

        @Override
        public void run() {
            // test if the server is a sudplan compatible server and has at least one entry in every relevant table
            try {
                final Object o = ms.performCustomSearch(STMT_TEST_SUDPLAN_SYSTEM);
            } catch (RemoteException ex) {
                // we caught an exception so we ignore this server
                LOG.info("RunFetcher: ignoring server since test for sudplan system failed: " + domain, ex); // NOI18N

                return;
            }

            // test was successful, now fetch RUN metaclass id
            final int runClassId;
            try {
                final MetaClass runClass = ms.getClassByTableName(getUser(), "run"); // NOI18N
                runClassId = runClass.getID();
            } catch (final Exception ex) {
                LOG.error("cannot fetch run metaclass", ex);                         // NOI18N
                exception = ex;

                return;
            }

            // now search for the runs
            final int[] runObjectIds;
            try {
                final ArrayList<ArrayList> results = ms.performCustomSearch(STMT_SEARCH_UNFINISHED_RUNS);
                runObjectIds = new int[results.size()];

                for (int i = 0; i < results.size(); ++i) {
                    final ArrayList al = results.get(i);
                    runObjectIds[i] = (Integer)al.get(0);
                }
            } catch (final Exception e) {
                LOG.error("cannot fetch unfinished runs", e); // NOI18N
                exception = e;

                return;
            }

            // finally build cidsbeans from the results
            try {
                for (final int runObjectId : runObjectIds) {
                    final MetaObject run = ms.getMetaObject(getUser(), runObjectId, runClassId);
                    result.add(run);
                }
            } catch (final Exception e) {
                LOG.error("cannot create metaobjects from found results", e); // NOI18N
                exception = e;

                return;
            }
        }
    }
}
