/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.trigger;

import it.geosolutions.geoserver.rest.encoder.utils.XmlElement;

/**
 * DOCUMENT ME!
 *
 * @author   bfriedrich
 * @version  $Revision$, $Date$
 */
public class GSAttributesEncoder extends XmlElement {

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new GSAttributesEncoder object.
     */
    public GSAttributesEncoder() {
        super("attributes");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param  attribute  DOCUMENT ME!
     */
    public void addAttribut(final GSAttributeEncoder attribute) {
        super.addContent(attribute.getRoot());
    }
}
