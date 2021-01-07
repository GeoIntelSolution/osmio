package org.speedup.datapart.vo.feature;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;


import java.util.Map;

public class Feature {
    private String type;
    @SerializedName(value = "geometry")
    private Geometry geometry;

    private Map<String,String> properties;

    public Feature() {
        this.type="Feature";
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Geometry getGeometry() {
        return geometry;
    }

    public void setGeometry(Geometry geometry) {
        this.geometry = geometry;
    }

    public void SetGeometry(String sw,String type){
        if(type.equalsIgnoreCase("Point")){
            geometry=new Gson().fromJson(sw,Point.class);
        }else if(type.equalsIgnoreCase("LineString")){
            geometry=new Gson().fromJson(sw,LineString.class);
        }else if(type.equalsIgnoreCase("Polygon")){
            geometry=new Gson().fromJson(sw,Polygon.class);
        }else if(type.equalsIgnoreCase("MultiPoint")){
            geometry=new Gson().fromJson(sw,MultiPoint.class);
        }else if(type.equalsIgnoreCase("MultiLineString")){
            geometry=new Gson().fromJson(sw,MultiLineString.class);
        }else if(type.equalsIgnoreCase("MultiPolygon")) {
            geometry = new Gson().fromJson(sw, MultiPolygon.class);
        }else {
            throw new RuntimeException(type+" Not implemented type yet!");
        }
    }
}
