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

import java.io.Serializable;

import java.rmi.RemoteException;

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
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class LightwightSwmmProjectsSearch extends CidsServerSearch {

    //~ Static fields/initializers ---------------------------------------------

    private static final transient Logger LOG = Logger.getLogger(LightwightSwmmProjectsSearch.class);
    private static final String STMT_TEST_SUDPLAN_SYSTEM = "SELECT DISTINCT c.id, r.id, s.id "
                + "FROM linz_cso c, run r, linz_swmm_result s "
                + "LIMIT 1";
    private static final String STMT_SWMM_PROJECTS =
        "SELECT id, title, description, inp_file_name FROM \"public\".swmm_project";

    //~ Instance fields --------------------------------------------------------

    protected final String searchName = "lightwight-swmm-projects-search";
    private final String domain;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LightwightSwmmProjectsSearch object.
     *
     * @param  domain  DOCUMENT ME!
     */
    public LightwightSwmmProjectsSearch(final String domain) {
        this.domain = domain;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public Collection<LightwightSwmmProject> performServerSearch() {
        if (!this.getActiveLoaclServers().containsKey(domain)) {
            LOG.error("user domain '" + domain + "' not supported!");
            return null;
        }

        final ExecutorService searcher = Executors.newCachedThreadPool(
                SudplanConcurrency.createThreadFactory(searchName)); // NOI18N
        final MetaService ms = (MetaService)this.getActiveLoaclServers().get(domain);
        final LightwightSwmmProjectsSearch.SwmmProjectFetcher fetcher =
            new LightwightSwmmProjectsSearch.SwmmProjectFetcher(ms, domain);

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
            final ArrayList<LightwightSwmmProject> swmmProjects = new ArrayList<LightwightSwmmProject>(
                    fetcherResults.size());

            for (final ArrayList row : fetcherResults) {
                final LightwightSwmmProject lightwightSwmmProject = new LightwightSwmmProject();
                if (row.size() != 4) {
                    LOG.warn("unexpected number of columsn in result array list: " + row.size());
                    continue;
                }

                if (row.get(0) != null) {
                    try {
                        lightwightSwmmProject.setId(Integer.valueOf(row.get(0).toString()));
                    } catch (Throwable t) {
                        LOG.error("could not get SWMM Project id from value '" + row.get(0) + "'", t);
                        continue;
                    }
                }

                lightwightSwmmProject.setTitle((row.get(1) != null) ? row.get(1).toString() : "");
                lightwightSwmmProject.setDescription((row.get(2) != null) ? row.get(2).toString() : "");
                lightwightSwmmProject.setInputFileName((row.get(3) != null) ? row.get(3).toString() : "");

                swmmProjects.add(lightwightSwmmProject);
            }

            return swmmProjects;
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
    private final class SwmmProjectFetcher implements Runnable {

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
        SwmmProjectFetcher(final MetaService ms, final String domain) {
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
                LOG.info("SwmmProjectFetcher: ignoring server since test for sudplan system failed: " + domain, ex); // NOI18N
                return;
            }

            try {
                final ArrayList<ArrayList> searchResult = ms.performCustomSearch(STMT_SWMM_PROJECTS);
                this.result = searchResult;
            } catch (RemoteException ex) {
                this.exception = ex;
                // we caught an exception so we ignore this server
                LOG.error("SwmmProjectFetcher: exception during execution of statement '"
                            + STMT_SWMM_PROJECTS + "' on domain '" + domain + "'",
                    ex); // NOI18N
            }
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @version  $Revision$, $Date$
     */
    public static class LightwightSwmmProject implements Serializable {

        //~ Instance fields ----------------------------------------------------

        private String title;
        private String description;

        private int id;

        private String inputFileName;

        //~ Methods ------------------------------------------------------------

        /**
         * Get the value of title.
         *
         * @return  the value of title
         */
        public String getTitle() {
            return title;
        }

        /**
         * Set the value of title.
         *
         * @param  title  new value of title
         */
        public void setTitle(final String title) {
            this.title = title;
        }

        /**
         * Get the value of description.
         *
         * @return  the value of description
         */
        public String getDescription() {
            return description;
        }

        /**
         * Set the value of description.
         *
         * @param  description  new value of description
         */
        public void setDescription(final String description) {
            this.description = description;
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
            return this.title;
        }

        /**
         * Get the value of inputFileName.
         *
         * @return  the value of inputFileName
         */
        public String getInputFileName() {
            return inputFileName;
        }

        /**
         * Set the value of inputFileName.
         *
         * @param  inputFileName  new value of inputFileName
         */
        public void setInputFileName(final String inputFileName) {
            this.inputFileName = inputFileName;
        }
    }
}
