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
import Sirius.server.middleware.types.MetaObjectNode;
import Sirius.server.middleware.types.Node;
import Sirius.server.search.CidsServerSearch;

import org.apache.log4j.Logger;

import java.rmi.RemoteException;

import java.text.DecimalFormat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import de.cismet.cids.custom.sudplan.commons.SudplanConcurrency;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class CsoByOverflowSearch extends CidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    private static final transient Logger LOG = Logger.getLogger(CsoByOverflowSearch.class);
    private static final String STMT_TEST_SUDPLAN_SYSTEM = "SELECT DISTINCT c.id, r.id, s.id "
                + "FROM linz_cso c, run r, linz_swmm_result s "
                + "LIMIT 1";
    private static final String STMT_CSO_BY_OVERFLOW_TEMPLATE = "SELECT DISTINCT  SWMM_RUN.linz_cso_reference  "
                + "FROM \"public\".linz_swmm_scenarios SWMM_RUN, \"public\".linz_swmm_result SWMM_RESULT "
                + "WHERE SWMM_RESULT.id = SWMM_RUN.linz_swmm_result  "
                + "AND SWMM_RESULT.overflow_volume <= %OVERFLOW% "
                + "AND SWMM_RESULT.swmm_scenario_id = ";

    public static final String DOMAIN = "SUDPLAN";

    //~ Instance fields --------------------------------------------------------

    protected String searchName = "cso-by-overflow-search";

    private final String csosWithoutOverflowStatement;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CsosWithoutOverflowSearch object.
     *
     * @param  swmmRunId       DOCUMENT ME!
     * @param  overflowVolume  DOCUMENT ME!
     */
    public CsoByOverflowSearch(final int swmmRunId, final float overflowVolume) {
        csosWithoutOverflowStatement = (STMT_CSO_BY_OVERFLOW_TEMPLATE.replace(
                    "%OVERFLOW%",
                    new DecimalFormat("##.##").format(overflowVolume)))
                    + swmmRunId;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public Collection performServerSearch() {
        final ExecutorService searcher = Executors.newCachedThreadPool(
                SudplanConcurrency.createThreadFactory(searchName)); // NOI18N

        final Map map = getActiveLoaclServers();
        final ArrayList<CsoByOverflowSearch.CsoFetcher> fetchers = new ArrayList<CsoByOverflowSearch.CsoFetcher>(
                map.size());
        for (final Object o : map.keySet()) {
            final String domain = (String)o;
            final MetaService ms = (MetaService)map.get(domain);

            final CsoByOverflowSearch.CsoFetcher fetcher = new CsoByOverflowSearch.CsoFetcher(ms, domain);
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

        final ArrayList<Node> csos = new ArrayList<Node>();
        for (final CsoByOverflowSearch.CsoFetcher fetcher : fetchers) {
            if (fetcher.getException() == null) {
                csos.addAll(fetcher.getResult());
            } else {
                LOG.error(
                    "at least one CsoFetcher terminated abnormally, search unsuccessful, returning", // NOI18N
                    fetcher.getException());

                return null;
            }
        }

        return csos;
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
        private final transient List<Node> result;
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
            this.result = new ArrayList<Node>();
        }

        //~ Methods ------------------------------------------------------------

        /**
         * DOCUMENT ME!
         *
         * @return  DOCUMENT ME!
         */
        List<Node> getResult() {
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

            final int csoClassId;
            try {
                final MetaClass csoClass = ms.getClassByTableName(getUser(), "linz_cso"); // NOI18N
                csoClassId = csoClass.getID();
            } catch (final Exception ex) {
                LOG.error("cannot fetch CSO metaclass (linz_cso)", ex);                   // NOI18N
                exception = ex;

                return;
            }

            // now search for the runs
            final int[] csoObjectIds;
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(csosWithoutOverflowStatement);
                }
                final ArrayList<ArrayList> results = ms.performCustomSearch(csosWithoutOverflowStatement);
                csoObjectIds = new int[results.size()];

                for (int i = 0; i < results.size(); ++i) {
                    final ArrayList al = results.get(i);
                    csoObjectIds[i] = (Integer)al.get(0);
                }
            } catch (final Exception e) {
                LOG.error("cannot fetch CSO", e); // NOI18N
                exception = e;

                return;
            }

            // finally build cidsbeans from the results
            try {
                for (final int csoObjectId : csoObjectIds) {
                    final MetaObject run = ms.getMetaObject(getUser(), csoObjectId, csoClassId);
                    final Node node = new MetaObjectNode(run.getBean());
                    result.add(node);
                }
            } catch (final Exception e) {
                LOG.error("cannot create metaobjects from found results", e); // NOI18N
                exception = e;
            }
        }
    }
}
