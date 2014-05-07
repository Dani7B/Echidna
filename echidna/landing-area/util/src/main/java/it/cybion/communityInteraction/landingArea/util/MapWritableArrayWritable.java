package it.cybion.communityInteraction.landingArea.util;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;

public class MapWritableArrayWritable extends ArrayWritable{

        public MapWritableArrayWritable() {
                super(MapWritable.class);
        }

}