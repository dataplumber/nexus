/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
/**
 * Created by Nga Quach on 8/31/16.
 */

if (timelen == null ){
    throw new RuntimeException("This script requires the length of the time array.")
}
if (timevar == null ){
    timevar = 'time'
}

def sectionSpec = ''

def length = timelen.toInteger()
for (i = 0; i < length; i++) {
    sectionSpec <<= timevar + ':' + i + ':' + (i+1) + ','
    sectionSpec <<= payload
    sectionSpec <<= ';'
}

sectionSpec <<= 'file://'
sectionSpec <<= headers.absolutefilepath

return sectionSpec.toString()
