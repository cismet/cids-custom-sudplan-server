/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.sudplan.server.trigger;

import it.geosolutions.geoserver.rest.encoder.feature.GSFeatureTypeEncoder;
import it.geosolutions.geoserver.rest.encoder.utils.XmlElement;

/**
 * DOCUMENT ME!
 *
 * @author   bfriedrich
 * @version  $Revision$, $Date$
 */
public class AttributesAwareGSFeatureTypeEncoder extends GSFeatureTypeEncoder {

    //~ Instance fields --------------------------------------------------------

    private final GSAttributesEncoder attributes;
    private String name;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new AttributesAwareGSFeatureTypeEncoder object.
     */
    public AttributesAwareGSFeatureTypeEncoder() {
        this.attributes = new GSAttributesEncoder();

        addContent(this.attributes.getRoot());
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param  attribute  DOCUMENT ME!
     */
    public void addAttribute(final GSAttributeEncoder attribute) {
        this.attributes.addAttribut(attribute);
    }

    @Override
    public void setName(final String name) {
        super.setName(name);
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }
}
