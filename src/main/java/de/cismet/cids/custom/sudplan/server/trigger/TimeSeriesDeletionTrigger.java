/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.trigger;

import Sirius.server.newuser.User;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.log4j.Logger;

import org.openide.util.lookup.ServiceProvider;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.cismet.cids.dynamics.CidsBean;

import de.cismet.cids.trigger.AbstractCidsTrigger;
import de.cismet.cids.trigger.CidsTrigger;
import de.cismet.cids.trigger.CidsTriggerKey;

/**
 * TimeSeriesDeletionTrigger deletes TimeSeries files (aggregated and "original") located on the WebDAV. If the deletion
 * of the remote files fails, the deletion of this TimeSeries object is aborted.
 *
 * @author   Benjamin Friedrich (benjamin.friedrich@cismet.de)
 * @version  1.0, 04.01.2012
 */
@ServiceProvider(service = CidsTrigger.class)
public class TimeSeriesDeletionTrigger extends AbstractCidsTrigger {

    //~ Static fields/initializers ---------------------------------------------

    public static final String DAV_HOST = "http://sudplan.cismet.de/tsDav/";
    public static final Credentials CREDS = new UsernamePasswordCredentials("tsDav", "RHfio2l4wrsklfghj");

    private static final String REGEX = "^dav:.+\\?.*ts:offering=(.+_unknown).*$";

    private static final Logger LOG = Logger.getLogger(TimeSeriesDeletionTrigger.class);

    //~ Instance fields --------------------------------------------------------

    private final HostConfiguration hostConfig;
    private HttpConnectionManager connectionManager;
    private final Pattern pattern;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new TimeSeriesDeletionTrigger object.
     */
    public TimeSeriesDeletionTrigger() {
        this.hostConfig = new HostConfiguration();
        this.hostConfig.setHost(DAV_HOST);
        this.connectionManager = new MultiThreadedHttpConnectionManager();
        final HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setMaxConnectionsPerHost(hostConfig, 20);
        this.connectionManager.setParams(params);

        this.pattern = Pattern.compile(REGEX);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    private HttpClient createHttpClient() {
        final HttpClient client = new HttpClient(this.connectionManager);
        client.setHostConfiguration(this.hostConfig);
        client.getState().setCredentials(AuthScope.ANY, CREDS);
        return client;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   uri     DOCUMENT ME!
     * @param   client  DOCUMENT ME!
     *
     * @throws  RuntimeException  DOCUMENT ME!
     */
    private void delete(final String uri, final HttpClient client) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Entering delete(String, User) with uri=" + uri + " client=" + client);
        }

        DeleteMethod del = null;

        try {
            del = new DeleteMethod(uri);
            client.executeMethod(del);
        } catch (final Exception ex) {
            LOG.error("An error occured while deleting remote file " + uri, ex);

            if (del != null) {
                del.abort();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Leaving delete(String, HttpClient) with error", ex);
            }
            throw new RuntimeException(ex);
        } finally {
            if (del != null) {
                del.releaseConnection();
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Leaving delete(String, HttpClient)");
        }
    }

    @Override
    public void beforeInsert(final CidsBean cidsBean, final User user) {
        // do nothing
    }

    @Override
    public void afterInsert(final CidsBean cidsBean, final User user) {
        // do nothing
    }

    @Override
    public void beforeUpdate(final CidsBean cidsBean, final User user) {
        // do nothing
    }

    @Override
    public void afterUpdate(final CidsBean cidsBean, final User user) {
        // do nothing
    }

    @Override
    public void beforeDelete(final CidsBean cidsBean, final User user) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Entering beforeDelete(CidsBean, User) with cidsBean=" + cidsBean + " user=" + user);
        }

        if (!"TIMESERIES".equalsIgnoreCase(cidsBean.getMetaObject().getMetaClass().getTableName())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Leaving beforeDelete(CidsBean, User) as cidsBean " + cidsBean
                            + " does not represent a TimeSeries");
            }
            return;
        }

        String uri = (String)cidsBean.getProperty("uri");

        final Matcher m = this.pattern.matcher(uri);
        if (m.matches()) {
            // group(1) = file name
            uri = DAV_HOST + m.group(1);
            final HttpClient client = this.createHttpClient();
            this.delete(uri, client);                                // delete TimeSeries file
            this.delete(uri.replace("_unknown", "_86400s"), client); // delete aggregated version of TimeSeries file
            if (LOG.isDebugEnabled()) {
                LOG.debug("Leaving beforeDelete(CidsBean, User)");
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Leaving beforeDelete(CidsBean, User) because TimeSeries uri " + uri
                            + " does not represent a remote file");
            }
        }
    }

    @Override
    public void afterDelete(final CidsBean cidsBean, final User user) {
        // do nothing
    }

    @Override
    public CidsTriggerKey getTriggerKey() {
        return CidsTriggerKey.FORALL; // new CidsTriggerKey(CidsTriggerKey.ALL, "TIMESERIES"); // NOI18N
    }

    /**
     * DOCUMENT ME!
     *
     * @param   t  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    @Override
    public int compareTo(final CidsTrigger t) {
        return -1;
    }

//    @Override
//    public void afterCommittedInsert(final CidsBean cidsBean, final User user) {
//    }
//
//    @Override
//    public void afterCommittedUpdate(final CidsBean cidsBean, final User user) {
//    }
//
//    @Override
//    public void afterCommittedDelete(final CidsBean cidsBean, final User user) {
//    }
}
