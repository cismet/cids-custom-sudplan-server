/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.search;

import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.search.CidsServerSearch;

import org.apache.log4j.Logger;

import java.io.Serializable;

import java.rmi.RemoteException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import de.cismet.cids.custom.sudplan.commons.SudplanConcurrency;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class LightwightCsoSearch extends CidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    private static final transient Logger LOG = Logger.getLogger(LightwightCsoSearch.class);
    private static final transient String STMT_TEST_SUDPLAN_SYSTEM = "SELECT DISTINCT c.id, r.id, s.id "
                + "FROM linz_cso c, run r, linz_swmm_result s "
                + "LIMIT 1";
    private static final transient String STMT_CSOS = "SELECT id, name FROM \"public\".linz_cso WHERE swmm_project = ";

    //~ Instance fields --------------------------------------------------------

    protected final transient String searchName = "lightwight-cso-search";
    private final transient String domain;
    private final transient int swmmProjectId;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LightwightSwmmProjectsSearch object.
     *
     * @param  domain         DOCUMENT ME!
     * @param  swmmProjectId  DOCUMENT ME!
     */
    public LightwightCsoSearch(final String domain, final int swmmProjectId) {
        this.domain = domain;
        this.swmmProjectId = swmmProjectId;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public Collection<LightwightCso> performServerSearch() {
        if (!this.getActiveLoaclServers().containsKey(domain)) {
            LOG.error("user domain '" + domain + "' not supported!");
            return null;
        }

        final ExecutorService searcher = Executors.newCachedThreadPool(
                SudplanConcurrency.createThreadFactory(searchName)); // NOI18N
        final MetaService ms = (MetaService)this.getActiveLoaclServers().get(domain);
        final LightwightCsoSearch.CsoFetcher fetcher = new LightwightCsoSearch.CsoFetcher(ms, domain);

        searcher.submit(fetcher);
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

        if (fetcher.getException() == null) {
            final ArrayList<ArrayList> fetcherResults = fetcher.getResult();
            final ArrayList<LightwightCso> csos = new ArrayList<LightwightCso>(
                    fetcherResults.size());

            for (final ArrayList row : fetcherResults) {
                final LightwightCso cso = new LightwightCso();
                if (row.size() != 2) {
                    LOG.warn("unexpected number of columns in result array list: " + row.size());
                    continue;
                }

                if (row.get(0) != null) {
                    try {
                        cso.setId(Integer.valueOf(row.get(0).toString()));
                    } catch (Throwable t) {
                        LOG.error("could not get CSO id from value '" + row.get(0) + "'", t);
                        continue;
                    }
                }

                cso.setName((row.get(1) != null) ? row.get(1).toString() : "");

                csos.add(cso);
            }

            return csos;
        } else {
            LOG.error(
                "SwmmProjectFetcher terminated abnormally, search unsuccessful, returning null", // NOI18N
                fetcher.getException());

            return null;
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @version  $Revision$, $Date$
     */
    private final class CsoFetcher implements Runnable {

        //~ Instance fields ----------------------------------------------------

        private final transient MetaService ms;
        private final transient String domain;
        private transient ArrayList<ArrayList> result;
        private transient Exception exception;

        //~ Constructors -------------------------------------------------------

        /**
         * Creates a new CsoFetcher object.
         *
         * @param  ms      DOCUMENT ME!
         * @param  domain  DOCUMENT ME!
         */
        CsoFetcher(final MetaService ms, final String domain) {
            this.ms = ms;
            this.domain = domain;
            this.exception = null;
        }

        //~ Methods ------------------------------------------------------------

        /**
         * DOCUMENT ME!
         *
         * @return  DOCUMENT ME!
         */
        ArrayList<ArrayList> getResult() {
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
                LOG.info("CsoFetcher: ignoring server since test for sudplan system failed: " + domain, ex); // NOI18N
                return;
            }

            final String statement = STMT_CSOS + swmmProjectId;
            try {
                final ArrayList<ArrayList> searchResult = ms.performCustomSearch(statement);
                this.result = searchResult;
            } catch (RemoteException ex) {
                this.exception = ex;
                // we caught an exception so we ignore this server
                LOG.error("CsoFetcher: exception during execution of statement '"
                            + statement + "' on domain '" + domain + "'",
                    ex); // NOI18N
            }
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @version  $Revision$, $Date$
     */
    public static class LightwightCso implements Serializable {

        //~ Instance fields ----------------------------------------------------

        private String name;

        private int id;

        //~ Methods ------------------------------------------------------------

        /**
         * Get the value of name.
         *
         * @return  the value of name
         */
        public String getName() {
            return name;
        }

        /**
         * Set the value of name.
         *
         * @param  name  new value of name
         */
        public void setName(final String name) {
            this.name = name;
        }

        /**
         * Get the value of id.
         *
         * @return  the value of id
         */
        public int getId() {
            return id;
        }

        /**
         * Set the value of id.
         *
         * @param  id  new value of id
         */
        public void setId(final int id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }
}
