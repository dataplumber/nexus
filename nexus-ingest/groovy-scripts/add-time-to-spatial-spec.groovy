/*****************************************************************************
* Copyright (c) 2016 Jet Propulsion Laboratory,
* California Institute of Technology.  All rights reserved
*****************************************************************************/
/**
 * Created by Nga Quach on 8/31/16.
 */

def sectionSpecList = []
def sectionSpec = ''

for (i = 0; i < 240; i++) {
    sectionSpec <<= 'time:' + i + ':' + (i+1) + ','
    sectionSpec <<= payload.join(';time:' + i + ':' + (i+1) + ',')
    sectionSpec <<= ';'
    sectionSpec <<= 'file://'
    sectionSpec <<= headers.absolutefilepath
    sectionSpecList.add(sectionSpec.toString())
    sectionSpec.length = 0
}

return sectionSpecList
