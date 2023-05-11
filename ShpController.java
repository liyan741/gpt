package com.seven7m.controller;

import cn.hutool.crypto.digest.BCrypt;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.seven7m.base.entity.ReturnDTO;
import com.seven7m.entity.FloodAreaInfo;
import com.seven7m.entity.sys.SysUser;
import com.seven7m.model.*;
import com.seven7m.model.entity.*;
import com.seven7m.model.shpEntity.*;
import com.seven7m.service.*;
import com.seven7m.service.impl.*;
import com.seven7m.utils.*;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.exception.ZipException;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
@RequestMapping(value = "/shp")
@Controller
public class ShpController {

    @Autowired
    private DikeInfoServiceImpl dikeInfoService;

    @Autowired
    private FloodAreaInfoServiceImpl floodAreaInfoService;

    @Autowired
    private GateInfoServiceImpl gateInfoService;

    @Autowired
    private DiverPointInfoServiceImpl diverPointInfoService;

    @Autowired
    private PumpInfoServiceImpl pumpInfoService;

    @Autowired
    private SafeZoneInfoServiceImpl safeZoneInfoService;

    @Autowired
    private SafePlfInfoServiceImpl safePlfInfoService;

    @Autowired
    private EarlyWarningSiteServiceImpl earlyWarningSiteService;

    @Autowired
    private SpaceDataServiceImpl spaceDataService;


    @Value("${bupload_path}")
    private String bUploadPath;

    @ApiOperation(value = "上传Zip——shape包解析", notes = "测试接口")
    @PostMapping(value = "/uploadZipShapeParse")
    @ResponseBody
    public ReturnDTO uploadZipShapeParse(@RequestBody Map map) {

        ArrayList<String> errorList = new ArrayList<>();
        HashMap<String, Object> maxMap = new HashMap<>();

// ==========================================表1 面数据==========================================1，6是面数据，2是线，3，4，5，7，8都是点
        try {
            File ShapeDir = new File(bUploadPath + "/" + map.get("zipName") + "/1.范围");

            if (!ShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }
            List<File> shapeFiles = new ArrayList<>();
            File[] childFiles = ShapeDir.listFiles();
            File tempFile = null;
            System.out.println("查找解压目录：" + ShapeDir + " 中 shape 文件");

            if (childFiles.length != 0) {
                // 过滤目录中shape文件
                for (int i = 0; i < childFiles.length; i++) {
                    tempFile = childFiles[i];
                    if (tempFile.getName().endsWith(".shp")) {
                        shapeFiles.add(tempFile);
                    }
                }
                System.out.println("shape 文件个数为 = " + shapeFiles.size());

                List<NewGeomShape.FLOOD_AREA_INFO> floodAreaInfoList = new ArrayList<>();

                System.out.println("遍历解析 shape 文件，解析数据");

                // 遍历shape文件读取数据
                for (int i = 0; i < shapeFiles.size(); i++) {
                    long start = System.currentTimeMillis();

                    // 使用GeoTools读取ShapeFile文件
                    File shapeFile = shapeFiles.get(i);
                    String shapeFileName = shapeFile.getName();
                    System.out.println("shapeFileName = " + shapeFileName);

                    ShapefileDataStore store = new ShapefileDataStore(shapeFile.toURI().toURL());
                    //设置编码
                    Charset charset = Charset.forName("GBK");
                    store.setCharset(charset);
                    SimpleFeatureSource sfSource = store.getFeatureSource();
                    SimpleFeatureIterator sfIter = sfSource.getFeatures().features();

                    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
                    while (sfIter.hasNext()) {
                        SimpleFeature feature = (SimpleFeature) sfIter.next();
                        // Feature转GeoJSON
                        FeatureJSON fjson = new FeatureJSON();
                        StringWriter writer = new StringWriter();
                        fjson.writeFeature(feature, writer);
                        String sjson = writer.toString();
//				System.out.println("sjson=====  >>>>  " + sjson);
                        JSONObject json = JSONObject.parseObject(sjson);
                        String jsonString = json.toJSONString();


                        NewGeomShape.FLOOD_AREA_INFO floodAreaInfo = JSON.parseObject(jsonString, NewGeomShape.FLOOD_AREA_INFO.class);
                        floodAreaInfoList.add(floodAreaInfo);


                        System.out.println("第" + i + "个文件数据导入完成，共耗时" + (System.currentTimeMillis() - start) + "ms");
                    }
                    sfIter.close();
                }

                System.out.println("floodAreaInfoList = " + floodAreaInfoList.size());
                if (floodAreaInfoList.size() > 0) {
                    List<FloodAreaInfoShp> insertList = new ArrayList<>();
                    FloodAreaInfoShp insertItem = null;
                    NewGeomShape.FLOOD_AREA_INFO orgDataItem = null;
                    for (int i = 0; i < floodAreaInfoList.size(); i++) {
                        insertItem = new FloodAreaInfoShp();
                        orgDataItem = floodAreaInfoList.get(i);
                        NewGeomShape.FLOOD_AREA_INFO.GeometryBean geometryBean = orgDataItem.getGeometry();
                        String geomType = geometryBean.getType();
                        System.out.println("geomType = " + geomType);
                        insertItem = NewGeomShape.toFloodAreaInfo(orgDataItem.getProperties());
                        insertItem.setGeom(GeometryUtils.geometryMultiPolygonToString(geometryBean.getCoordinates()));
                        insertList.add(insertItem);

                        // 解析完成后根据堤防编码 存入对应的数据中
                        String id = floodAreaInfoService.getIdByDacd(insertItem.getDacd());
                        if (id == null) {
                            errorList.add("基础信息台账表中没有蓄滞洪区编码为:" + insertItem.getDacd() + "的记录");
                        }
                        floodAreaInfoService.updateGeomByDacd(insertItem.getDacd(), insertItem.getGeom());
                    }

                    System.out.println("表1 解析完成 ");
                }
            }
// ==========================================表2 线数据===============================================================================
            File dfShapeDir = new File(bUploadPath + "/" + map.get("zipName") + "/2.堤防");

            if (!dfShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }
            List<File> dfShapeFiles = new ArrayList<>();
            File[] dfChildFiles = dfShapeDir.listFiles();
            File dfTempFile = null;
            System.out.println("查找解压目录：" + dfShapeDir + " 中 shape 文件");

            if (dfChildFiles.length != 0) {
                // 过滤目录中shape文件
                for (int i = 0; i < dfChildFiles.length; i++) {
                    dfTempFile = dfChildFiles[i];
                    if (dfTempFile.getName().endsWith(".shp")) {
                        dfShapeFiles.add(dfTempFile);
                    }
                }
                System.out.println("shape 文件个数为 = " + dfShapeFiles.size());

                List<NewGeomShape.DIKE_INFO> dikeInfoList = new ArrayList<>();

                System.out.println("遍历解析 shape 文件，解析数据");

                // 遍历shape文件读取数据
                for (int i = 0; i < dfShapeFiles.size(); i++) {
                    long start = System.currentTimeMillis();

                    // 使用GeoTools读取ShapeFile文件
                    File shapeFile = dfShapeFiles.get(i);
                    String shapeFileName = shapeFile.getName();
                    System.out.println("shapeFileName = " + shapeFileName);

                    ShapefileDataStore store = new ShapefileDataStore(shapeFile.toURI().toURL());
                    //设置编码
                    Charset charset = Charset.forName("GBK");
                    store.setCharset(charset);
                    SimpleFeatureSource sfSource = store.getFeatureSource();
                    SimpleFeatureIterator sfIter = sfSource.getFeatures().features();

                    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
                    while (sfIter.hasNext()) {
                        SimpleFeature feature = (SimpleFeature) sfIter.next();
                        // Feature转GeoJSON
                        FeatureJSON fjson = new FeatureJSON();
                        StringWriter writer = new StringWriter();
                        fjson.writeFeature(feature, writer);
                        String sjson = writer.toString();
//				System.out.println("sjson=====  >>>>  " + sjson);
                        JSONObject json = JSONObject.parseObject(sjson);
                        String jsonString = json.toJSONString();


                        NewGeomShape.DIKE_INFO dikeInfo = JSON.parseObject(jsonString, NewGeomShape.DIKE_INFO.class);
                        dikeInfoList.add(dikeInfo);


                        System.out.println("第" + i + "个文件数据导入完成，共耗时" + (System.currentTimeMillis() - start) + "ms");
                    }
                    sfIter.close();
                }

                System.out.println("dikeInfoList = " + dikeInfoList.size());
                if (dikeInfoList.size() > 0) {
                    List<DikeInfoShp> insertList = new ArrayList<>();
                    DikeInfoShp insertItem = null;
                    NewGeomShape.DIKE_INFO orgDataItem = null;
                    for (int i = 0; i < dikeInfoList.size(); i++) {
                        insertItem = new DikeInfoShp();
                        orgDataItem = dikeInfoList.get(i);
                        NewGeomShape.DIKE_INFO.GeometryBean geometryBean = orgDataItem.getGeometry();
                        String geomType = geometryBean.getType();
                        System.out.println("geomType = " + geomType);
                        insertItem = NewGeomShape.toDikeInfoShp(orgDataItem.getProperties());
                        insertItem.setGeom(GeometryUtils.geometryMultiLineStringToString(geometryBean.getCoordinates()));
                        insertList.add(insertItem);
                        System.out.println(insertItem);


                        // 解析完成后根据堤防编码 存入对应的数据中
                        String id = dikeInfoService.getDikeByDkcd(insertItem.getDkcd());
                        if (id == null) {
                            errorList.add("堤防台账表中没有堤防编码为:" + insertItem.getDkcd() + "的记录");
                        }
                        dikeInfoService.addGeomByDkcd(insertItem.getDkcd(), insertItem.getGeom());
                    }

                    System.out.println("表2 解析完成 ");
                }
            }
// ==========================================表3 点数据===============================================================================

            File szShapeDir = new File(bUploadPath + "/" + map.get("zipName") + "/3.水闸");

            if (!szShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }
            List<File> szShapeFiles = new ArrayList<>();
            File[] szChildFiles = szShapeDir.listFiles();
            File szTempFile = null;
            System.out.println("查找解压目录：" + szShapeDir + " 中 shape 文件");

            if (szChildFiles.length != 0) {
                // 过滤目录中shape文件
                for (int i = 0; i < szChildFiles.length; i++) {
                    szTempFile = szChildFiles[i];
                    if (szTempFile.getName().endsWith(".shp")) {
                        szShapeFiles.add(szTempFile);
                    }
                }
                System.out.println("shape 文件个数为 = " + szShapeFiles.size());

                List<NewGeomShape.GATE_INFO> gateInfoList = new ArrayList<>();

                System.out.println("遍历解析 shape 文件，解析数据");

                // 遍历shape文件读取数据
                for (int i = 0; i < szShapeFiles.size(); i++) {
                    long start = System.currentTimeMillis();

                    // 使用GeoTools读取ShapeFile文件
                    File shapeFile = szShapeFiles.get(i);
                    String shapeFileName = shapeFile.getName();
                    System.out.println("shapeFileName = " + shapeFileName);

                    ShapefileDataStore store = new ShapefileDataStore(shapeFile.toURI().toURL());
                    //设置编码
                    Charset charset = Charset.forName("GBK");
                    store.setCharset(charset);
                    SimpleFeatureSource sfSource = store.getFeatureSource();
                    SimpleFeatureIterator sfIter = sfSource.getFeatures().features();

                    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
                    while (sfIter.hasNext()) {
                        SimpleFeature feature = (SimpleFeature) sfIter.next();
                        // Feature转GeoJSON
                        FeatureJSON fjson = new FeatureJSON();
                        StringWriter writer = new StringWriter();
                        fjson.writeFeature(feature, writer);
                        String sjson = writer.toString();
//				System.out.println("sjson=====  >>>>  " + sjson);
                        JSONObject json = JSONObject.parseObject(sjson);
                        String jsonString = json.toJSONString();

                        NewGeomShape.GATE_INFO gateInfo = JSON.parseObject(jsonString, NewGeomShape.GATE_INFO.class);
                        gateInfoList.add(gateInfo);


                        System.out.println("第" + i + "个文件数据导入完成，共耗时" + (System.currentTimeMillis() - start) + "ms");
                    }
                    sfIter.close();
                }

                System.out.println("gateInfoList = " + gateInfoList.size());
                if (gateInfoList.size() > 0) {
                    List<GateInfoShp> insertList = new ArrayList<>();
                    GateInfoShp insertItem = null;
                    NewGeomShape.GATE_INFO orgDataItem = null;
                    for (int i = 0; i < gateInfoList.size(); i++) {
                        insertItem = new GateInfoShp();
                        orgDataItem = gateInfoList.get(i);
                        NewGeomShape.GATE_INFO.GeometryBean geometryBean = orgDataItem.getGeometry();
                        String geomType = geometryBean.getType();
                        System.out.println("geomType = " + geomType);
                        insertItem = NewGeomShape.toGateInfoShp(orgDataItem.getProperties());
                        insertItem.setGeom(GeometryUtils.geometryPointToString(geometryBean.getCoordinates()));
                        insertList.add(insertItem);


                        // 解析完成后根据堤防编码 存入对应的数据中
                        String id = gateInfoService.getIdByGtcd(insertItem.getGtcd());
                        if (id == null) {
                            errorList.add("水闸台账表中没有水闸编码为:" + insertItem.getGtcd() + "的记录");
                        }
                        gateInfoService.updateGeomByGtcd(insertItem.getGtcd(), insertItem.getGeom());
                    }

                    System.out.println("表3 解析完成 ");
                }
            }
// ==========================================表4 点数据===============================================================================

            File kmShapeDir = new File(bUploadPath + "/" + map.get("zipName") + "/4.口门");

            if (!kmShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }
            List<File> kmShapeFiles = new ArrayList<>();
            File[] kmChildFiles = kmShapeDir.listFiles();
            File kmTempFile = null;
            System.out.println("查找解压目录：" + kmShapeDir + " 中 shape 文件");

            if (kmChildFiles.length != 0) {
                // 过滤目录中shape文件
                for (int i = 0; i < kmChildFiles.length; i++) {
                    kmTempFile = kmChildFiles[i];
                    if (kmTempFile.getName().endsWith(".shp")) {
                        kmShapeFiles.add(kmTempFile);
                    }
                }
                System.out.println("shape 文件个数为 = " + kmShapeFiles.size());

                List<NewGeomShape.DIVER_POINT_INFO> diverPointInfoList = new ArrayList<>();

                System.out.println("遍历解析 shape 文件，解析数据");

                // 遍历shape文件读取数据
                for (int i = 0; i < kmShapeFiles.size(); i++) {
                    long start = System.currentTimeMillis();

                    // 使用GeoTools读取ShapeFile文件
                    File shapeFile = kmShapeFiles.get(i);
                    String shapeFileName = shapeFile.getName();
                    System.out.println("shapeFileName = " + shapeFileName);

                    ShapefileDataStore store = new ShapefileDataStore(shapeFile.toURI().toURL());
                    //设置编码
                    Charset charset = Charset.forName("GBK");
                    store.setCharset(charset);
                    SimpleFeatureSource sfSource = store.getFeatureSource();
                    SimpleFeatureIterator sfIter = sfSource.getFeatures().features();

                    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
                    while (sfIter.hasNext()) {
                        SimpleFeature feature = (SimpleFeature) sfIter.next();
                        // Feature转GeoJSON
                        FeatureJSON fjson = new FeatureJSON();
                        StringWriter writer = new StringWriter();
                        fjson.writeFeature(feature, writer);
                        String sjson = writer.toString();
//				System.out.println("sjson=====  >>>>  " + sjson);
                        JSONObject json = JSONObject.parseObject(sjson);
                        String jsonString = json.toJSONString();

                        NewGeomShape.DIVER_POINT_INFO diverPointInfo = JSON.parseObject(jsonString, NewGeomShape.DIVER_POINT_INFO.class);
                        diverPointInfoList.add(diverPointInfo);


                        System.out.println("第" + i + "个文件数据导入完成，共耗时" + (System.currentTimeMillis() - start) + "ms");
                    }
                    sfIter.close();
                }

                System.out.println("diverPointInfoList = " + diverPointInfoList.size());
                if (diverPointInfoList.size() > 0) {
                    List<DiverPointInfoShp> insertList = new ArrayList<>();
                    DiverPointInfoShp insertItem = null;
                    NewGeomShape.DIVER_POINT_INFO orgDataItem = null;
                    for (int i = 0; i < diverPointInfoList.size(); i++) {
                        insertItem = new DiverPointInfoShp();
                        orgDataItem = diverPointInfoList.get(i);
                        NewGeomShape.DIVER_POINT_INFO.GeometryBean geometryBean = orgDataItem.getGeometry();
                        String geomType = geometryBean.getType();
                        System.out.println("geomType = " + geomType);
                        insertItem = NewGeomShape.toDiverPointInfoShp(orgDataItem.getProperties());
                        insertItem.setGeom(GeometryUtils.geometryPointToString(geometryBean.getCoordinates()));
                        insertList.add(insertItem);


                        // 解析完成后根据堤防编码 存入对应的数据中
                        String id = diverPointInfoService.getIdByDpcd(insertItem.getDpcd());
                        if (id == null) {
                            errorList.add("口门台账表中没有口门编码为:" + insertItem.getDpcd() + "的记录");
                        }
                        diverPointInfoService.updateGeomByDpcd(insertItem.getDpcd(), insertItem.getGeom());
                    }

                    System.out.println("表4 解析完成 ");
                }
            }

// ==========================================表5 点数据===============================================================================

            File bzShapeDir = new File(bUploadPath + "/" + map.get("zipName") + "/5.泵站");

            if (!bzShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }
            List<File> bzShapeFiles = new ArrayList<>();
            File[] bzChildFiles = bzShapeDir.listFiles();
            File bzTempFile = null;
            System.out.println("查找解压目录：" + bzShapeDir + " 中 shape 文件");

            if (bzChildFiles.length != 0) {
                // 过滤目录中shape文件
                for (int i = 0; i < bzChildFiles.length; i++) {
                    bzTempFile = bzChildFiles[i];
                    if (bzTempFile.getName().endsWith(".shp")) {
                        bzShapeFiles.add(bzTempFile);
                    }
                }
                System.out.println("shape 文件个数为 = " + bzShapeFiles.size());

                List<NewGeomShape.PUMP_INFO> pumpInfoList = new ArrayList<>();

                System.out.println("遍历解析 shape 文件，解析数据");

                // 遍历shape文件读取数据
                for (int i = 0; i < bzShapeFiles.size(); i++) {
                    long start = System.currentTimeMillis();

                    // 使用GeoTools读取ShapeFile文件
                    File shapeFile = bzShapeFiles.get(i);
                    String shapeFileName = shapeFile.getName();
                    System.out.println("shapeFileName = " + shapeFileName);

                    ShapefileDataStore store = new ShapefileDataStore(shapeFile.toURI().toURL());
                    //设置编码
                    Charset charset = Charset.forName("GBK");
                    store.setCharset(charset);
                    SimpleFeatureSource sfSource = store.getFeatureSource();
                    SimpleFeatureIterator sfIter = sfSource.getFeatures().features();

                    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
                    while (sfIter.hasNext()) {
                        SimpleFeature feature = (SimpleFeature) sfIter.next();
                        // Feature转GeoJSON
                        FeatureJSON fjson = new FeatureJSON();
                        StringWriter writer = new StringWriter();
                        fjson.writeFeature(feature, writer);
                        String sjson = writer.toString();
//				System.out.println("sjson=====  >>>>  " + sjson);
                        JSONObject json = JSONObject.parseObject(sjson);
                        String jsonString = json.toJSONString();

                        NewGeomShape.PUMP_INFO diverPointInfo = JSON.parseObject(jsonString, NewGeomShape.PUMP_INFO.class);
                        pumpInfoList.add(diverPointInfo);


                        System.out.println("第" + i + "个文件数据导入完成，共耗时" + (System.currentTimeMillis() - start) + "ms");
                    }
                    sfIter.close();
                }

                System.out.println("pumpInfoList = " + pumpInfoList.size());
                if (pumpInfoList.size() > 0) {
                    List<PumpInfoShp> insertList = new ArrayList<>();
                    PumpInfoShp insertItem = null;
                    NewGeomShape.PUMP_INFO orgDataItem = null;
                    for (int i = 0; i < pumpInfoList.size(); i++) {
                        insertItem = new PumpInfoShp();
                        orgDataItem = pumpInfoList.get(i);
                        NewGeomShape.PUMP_INFO.GeometryBean geometryBean = orgDataItem.getGeometry();
                        String geomType = geometryBean.getType();
                        System.out.println("geomType = " + geomType);
                        insertItem = NewGeomShape.toPumpInfoShp(orgDataItem.getProperties());
                        insertItem.setGeom(GeometryUtils.geometryPointToString(geometryBean.getCoordinates()));
                        insertList.add(insertItem);


                        // 解析完成后根据堤防编码 存入对应的数据中
                        String id = pumpInfoService.getIdByPpcd(insertItem.getPpcd());
                        if (id == null) {
                            errorList.add("泵站台账表中没有泵站编码为:" + insertItem.getPpcd() + "的记录");
                        }
                        pumpInfoService.updateGeomByPpcd(insertItem.getPpcd(), insertItem.getGeom());
                    }

                    System.out.println("表5 解析完成 ");
                }
            }
// ==========================================表6 面数据===============================================================================

            File aqqShapeDir = new File(bUploadPath + "/" + map.get("zipName") + "/6.安全区");

            if (!aqqShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }
            List<File> aqqShapeFiles = new ArrayList<>();
            File[] aqqChildFiles = aqqShapeDir.listFiles();
            File aqqTempFile = null;
            System.out.println("查找解压目录：" + aqqShapeDir + " 中 shape 文件");

            if (aqqChildFiles.length != 0) {
                // 过滤目录中shape文件
                for (int i = 0; i < aqqChildFiles.length; i++) {
                    aqqTempFile = aqqChildFiles[i];
                    if (aqqTempFile.getName().endsWith(".shp")) {
                        aqqShapeFiles.add(aqqTempFile);
                    }
                }
                System.out.println("shape 文件个数为 = " + aqqShapeFiles.size());

                List<NewGeomShape.SAFE_ZONE_INFO> safeZoneInfoList = new ArrayList<>();

                System.out.println("遍历解析 shape 文件，解析数据");

                // 遍历shape文件读取数据
                for (int i = 0; i < aqqShapeFiles.size(); i++) {
                    long start = System.currentTimeMillis();

                    // 使用GeoTools读取ShapeFile文件
                    File shapeFile = aqqShapeFiles.get(i);
                    String shapeFileName = shapeFile.getName();
                    System.out.println("shapeFileName = " + shapeFileName);

                    ShapefileDataStore store = new ShapefileDataStore(shapeFile.toURI().toURL());
                    //设置编码
                    Charset charset = Charset.forName("GBK");
                    store.setCharset(charset);
                    SimpleFeatureSource sfSource = store.getFeatureSource();
                    SimpleFeatureIterator sfIter = sfSource.getFeatures().features();

                    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
                    while (sfIter.hasNext()) {
                        SimpleFeature feature = (SimpleFeature) sfIter.next();
                        // Feature转GeoJSON
                        FeatureJSON fjson = new FeatureJSON();
                        StringWriter writer = new StringWriter();
                        fjson.writeFeature(feature, writer);
                        String sjson = writer.toString();
//				System.out.println("sjson=====  >>>>  " + sjson);
                        JSONObject json = JSONObject.parseObject(sjson);
                        String jsonString = json.toJSONString();

                        NewGeomShape.SAFE_ZONE_INFO safeZoneInfo = JSON.parseObject(jsonString, NewGeomShape.SAFE_ZONE_INFO.class);
                        safeZoneInfoList.add(safeZoneInfo);


                        System.out.println("第" + i + "个文件数据导入完成，共耗时" + (System.currentTimeMillis() - start) + "ms");
                    }
                    sfIter.close();
                }

                System.out.println("safeZoneInfoList = " + safeZoneInfoList.size());
                if (safeZoneInfoList.size() > 0) {
                    List<SafeZoneInfoShp> insertList = new ArrayList<>();
                    SafeZoneInfoShp insertItem = null;
                    NewGeomShape.SAFE_ZONE_INFO orgDataItem = null;
                    for (int i = 0; i < safeZoneInfoList.size(); i++) {
                        insertItem = new SafeZoneInfoShp();
                        orgDataItem = safeZoneInfoList.get(i);
                        NewGeomShape.SAFE_ZONE_INFO.GeometryBean geometryBean = orgDataItem.getGeometry();
                        String geomType = geometryBean.getType();
                        System.out.println("geomType = " + geomType);
                        insertItem = NewGeomShape.toSafeZoneInfoShp(orgDataItem.getProperties());
                        insertItem.setGeom(GeometryUtils.geometryMultiPolygonToString(geometryBean.getCoordinates()));
                        insertList.add(insertItem);
                        System.out.println(insertItem);


                        // 解析完成后根据堤防编码 存入对应的数据中
                        String id = safeZoneInfoService.getIdBySzcd(insertItem.getSzcd());
                        if (id == null) {
                            errorList.add("安全区台账表中没有安全区编码为:" + insertItem.getSzcd() + "的记录");
                        }
                        safeZoneInfoService.updateGeomBySzcd(insertItem.getSzcd(), insertItem.getGeom());
                    }

                    System.out.println("表6 解析完成 ");
                }
            }

// ==========================================表7 点数据===============================================================================

            File aqtShapeDir = new File(bUploadPath + "/" + map.get("zipName") + "/7.安全台");

            if (!aqtShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }
            List<File> aqtShapeFiles = new ArrayList<>();
            File[] aqtChildFiles = aqtShapeDir.listFiles();
            File aqtTempFile = null;
            System.out.println("查找解压目录：" + aqtShapeDir + " 中 shape 文件");

            if (aqtChildFiles.length != 0) {
                // 过滤目录中shape文件
                for (int i = 0; i < aqtChildFiles.length; i++) {
                    aqtTempFile = aqtChildFiles[i];
                    if (aqtTempFile.getName().endsWith(".shp")) {
                        aqtShapeFiles.add(aqtTempFile);
                    }
                }
                System.out.println("shape 文件个数为 = " + aqtShapeFiles.size());

                List<NewGeomShape.SAFE_PLF_INFO> safePlfInfoList = new ArrayList<>();

                System.out.println("遍历解析 shape 文件，解析数据");

                // 遍历shape文件读取数据
                for (int i = 0; i < aqtShapeFiles.size(); i++) {
                    long start = System.currentTimeMillis();

                    // 使用GeoTools读取ShapeFile文件
                    File shapeFile = aqtShapeFiles.get(i);
                    String shapeFileName = shapeFile.getName();
                    System.out.println("shapeFileName = " + shapeFileName);

                    ShapefileDataStore store = new ShapefileDataStore(shapeFile.toURI().toURL());
                    //设置编码
                    Charset charset = Charset.forName("GBK");
                    store.setCharset(charset);
                    SimpleFeatureSource sfSource = store.getFeatureSource();
                    SimpleFeatureIterator sfIter = sfSource.getFeatures().features();

                    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
                    while (sfIter.hasNext()) {
                        SimpleFeature feature = (SimpleFeature) sfIter.next();
                        // Feature转GeoJSON
                        FeatureJSON fjson = new FeatureJSON();
                        StringWriter writer = new StringWriter();
                        fjson.writeFeature(feature, writer);
                        String sjson = writer.toString();
//				System.out.println("sjson=====  >>>>  " + sjson);
                        JSONObject json = JSONObject.parseObject(sjson);
                        String jsonString = json.toJSONString();

                        NewGeomShape.SAFE_PLF_INFO safePlfInfo = JSON.parseObject(jsonString, NewGeomShape.SAFE_PLF_INFO.class);
                        safePlfInfoList.add(safePlfInfo);


                        System.out.println("第" + i + "个文件数据导入完成，共耗时" + (System.currentTimeMillis() - start) + "ms");
                    }
                    sfIter.close();
                }

                System.out.println("safePlfInfoList = " + safePlfInfoList.size());
                if (safePlfInfoList.size() > 0) {
                    List<SafePlfInfoShp> insertList = new ArrayList<>();
                    SafePlfInfoShp insertItem = null;
                    NewGeomShape.SAFE_PLF_INFO orgDataItem = null;
                    for (int i = 0; i < safePlfInfoList.size(); i++) {
                        insertItem = new SafePlfInfoShp();
                        orgDataItem = safePlfInfoList.get(i);
                        NewGeomShape.SAFE_PLF_INFO.GeometryBean geometryBean = orgDataItem.getGeometry();
                        String geomType = geometryBean.getType();
                        System.out.println("geomType = " + geomType);
                        insertItem = NewGeomShape.toSafePlfInfoShp(orgDataItem.getProperties());
                        insertItem.setGeom(GeometryUtils.geometryPointToString(geometryBean.getCoordinates()));
                        insertList.add(insertItem);


                        // 解析完成后根据堤防编码 存入对应的数据中
                        String id = safePlfInfoService.getIdBySpcd(insertItem.getSpcd());
                        if (id == null) {
                            errorList.add("安全台台账表中没有安全台编码为:" + insertItem.getSpcd() + "的记录");
                        }
                        safePlfInfoService.updateGeomBySpcd(insertItem.getSpcd(), insertItem.getGeom());
                    }

                    System.out.println("表7 解析完成 ");
                }
            }

// ==========================================表8 点数据===============================================================================

            File yjzdShapeDir = new File(bUploadPath + "/" + map.get("zipName") + "/8.预警站点");

            if (!yjzdShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }
            List<File> yjzdShapeFiles = new ArrayList<>();
            File[] yjzdChildFiles = yjzdShapeDir.listFiles();
            File yjzdTempFile = null;
            System.out.println("查找解压目录：" + yjzdShapeDir + " 中 shape 文件");

            if (yjzdChildFiles.length != 0) {
                // 过滤目录中shape文件
                for (int i = 0; i < yjzdChildFiles.length; i++) {
                    yjzdTempFile = yjzdChildFiles[i];
                    if (yjzdTempFile.getName().endsWith(".shp")) {
                        yjzdShapeFiles.add(yjzdTempFile);
                    }
                }
                System.out.println("shape 文件个数为 = " + yjzdShapeFiles.size());

                List<NewGeomShape.EARLY_WARNING_SITE> earlyWarningSiteList = new ArrayList<>();

                System.out.println("遍历解析 shape 文件，解析数据");

                // 遍历shape文件读取数据
                for (int i = 0; i < yjzdShapeFiles.size(); i++) {
                    long start = System.currentTimeMillis();

                    // 使用GeoTools读取ShapeFile文件
                    File shapeFile = yjzdShapeFiles.get(i);
                    String shapeFileName = shapeFile.getName();
                    System.out.println("shapeFileName = " + shapeFileName);

                    ShapefileDataStore store = new ShapefileDataStore(shapeFile.toURI().toURL());
                    //设置编码
                    Charset charset = Charset.forName("GBK");
                    store.setCharset(charset);
                    SimpleFeatureSource sfSource = store.getFeatureSource();
                    SimpleFeatureIterator sfIter = sfSource.getFeatures().features();

                    // 从ShapeFile文件中遍历每一个Feature，然后将Feature转为GeoJSON字符串
                    while (sfIter.hasNext()) {
                        SimpleFeature feature = (SimpleFeature) sfIter.next();
                        // Feature转GeoJSON
                        FeatureJSON fjson = new FeatureJSON();
                        StringWriter writer = new StringWriter();
                        fjson.writeFeature(feature, writer);
                        String sjson = writer.toString();
//				System.out.println("sjson=====  >>>>  " + sjson);
                        JSONObject json = JSONObject.parseObject(sjson);
                        String jsonString = json.toJSONString();

                        NewGeomShape.EARLY_WARNING_SITE earlyWarningSite = JSON.parseObject(jsonString, NewGeomShape.EARLY_WARNING_SITE.class);
                        earlyWarningSiteList.add(earlyWarningSite);


                        System.out.println("第" + i + "个文件数据导入完成，共耗时" + (System.currentTimeMillis() - start) + "ms");
                    }
                    sfIter.close();
                }

                System.out.println("earlyWarningSiteList = " + earlyWarningSiteList.size());
                if (earlyWarningSiteList.size() > 0) {
                    List<EarlyWarningSiteShp> insertList = new ArrayList<>();
                    EarlyWarningSiteShp insertItem = null;
                    NewGeomShape.EARLY_WARNING_SITE orgDataItem = null;
                    for (int i = 0; i < earlyWarningSiteList.size(); i++) {
                        insertItem = new EarlyWarningSiteShp();
                        orgDataItem = earlyWarningSiteList.get(i);
                        NewGeomShape.EARLY_WARNING_SITE.GeometryBean geometryBean = orgDataItem.getGeometry();
                        String geomType = geometryBean.getType();
                        System.out.println("geomType = " + geomType);
                        insertItem = NewGeomShape.toEarlyWarningSiteShp(orgDataItem.getProperties());
                        insertItem.setGeom(GeometryUtils.geometryPointToString(geometryBean.getCoordinates()));
                        insertList.add(insertItem);


                        // 解析完成后根据堤防编码 存入对应的数据中
                        String id = earlyWarningSiteService.getIdByStcd(insertItem.getStcd());
                        if (id == null) {
                            errorList.add("预警站点台账表中没有站点编码为:" + insertItem.getStcd() + "的记录");
                        }
                        earlyWarningSiteService.updateGeomByStcd(insertItem.getStcd(), insertItem.getGeom());
                    }

                    System.out.println("表8 解析完成 ");
                }
            }

            deleteFolder(new File(bUploadPath + "/" + map.get("zipName")));

        } catch (IOException e) {
            log.info("文件上传异常={}", e);
            return ReturnDTOUtil.error("文件上传异常={}" + e.getMessage());
        }
        BasinSpaceData basinSpaceData = new BasinSpaceData();
        if (map.get("num") != null) {
            basinSpaceData.setDacd((String) map.get("dacd"));
            basinSpaceData.setDanm((String) map.get("danm"));
            basinSpaceData.setContent((String) map.get("content"));
            basinSpaceData.setDeptName((String) map.get("deptName"));
            basinSpaceData.setFilePath(bUploadPath + map.get("zipName"));
            spaceDataService.addBasin(basinSpaceData);
        } else {
            Map<String, Object> objectMap = (Map<String, Object>) map.get("basinSpaceData");
            basinSpaceData.setDacd((String) objectMap.get("dacd"));
            basinSpaceData.setDanm((String) objectMap.get("danm"));
            basinSpaceData.setContent((String) objectMap.get("content"));
            basinSpaceData.setDeptName((String) objectMap.get("deptName"));
            basinSpaceData.setFilePath(bUploadPath + map.get("zipName"));
            spaceDataService.addBasin(basinSpaceData);
        }
        maxMap.put("errorList", errorList);
        maxMap.put("type", 1);
        return ReturnDTOUtil.success(maxMap);
    }


    @ApiOperation(value = "上传Zip——shape包解析", notes = "测试接口")
    @PostMapping(value = "/getZipType")
    @ResponseBody
    public ReturnDTO getZipType(BasinSpaceData basinSpaceData, @RequestParam("files") MultipartFile[] files) {

        if (basinSpaceData.getDeleteType() != null && basinSpaceData.getDeleteType() == 1) {
            String zipName = basinSpaceData.getZipName().split("\\.")[0];

            deleteFolder(new File(bUploadPath + "/" + zipName));
            System.out.println(bUploadPath + zipName);
            return ReturnDTOUtil.success();
        }

        // 存储上传成功的文件名，响应给客户端
        List<String> list = new ArrayList<>();

        // 空文件夹字符串集合
        ArrayList<String> typeList = new ArrayList<>();

        // 返回状态和压缩包名称
        Map<String, Object> map = new HashMap<>();

        // 判断文件数组长度
        if (files.length <= 0) {
            list.add("请选择文件");
            return ReturnDTOUtil.error("请选择文件");
        }
        for (MultipartFile file : files) {
            // 源文件名
            String originalFilename = file.getOriginalFilename();

            String zipName = originalFilename.split("\\.")[0];

            map.put("zipName", zipName);
            // 文件格式
            String suffix = originalFilename.substring(originalFilename.lastIndexOf("."));
            // 新文件名，避免文件名重复，造成文件替换问题
//			String fileName = UUID.randomUUID() + "." + suffix;
            String fileName = originalFilename;
            // 文件存储路径
            String tartgetPath = bUploadPath;
            File tartgetDir = new File(tartgetPath);
            // 判断文件存储目录是否存在，不存在则新建目录
            if (!tartgetDir.exists()) {
                tartgetDir.mkdir();
            }
            // 文件全路径
            File targetFile = new File(tartgetDir, fileName);

            // 将图片保存
            try {
                file.transferTo(targetFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println("文件上传保存成功：" + targetFile.getPath());
            list.add(originalFilename);

            System.out.println("开始Zip文件解压...");
            // 解压压缩包
            String unZipPath = tartgetPath + originalFilename.substring(0, originalFilename.lastIndexOf("."));
            String unZipPassword = "UxVssb9vV3Kz9bSLe3CCRw==";
            File zipFile = targetFile;
            if (zipFile.exists()) {
                try {
                    DecryptionZipUtil.unzip(zipFile, unZipPath, unZipPassword);
                } catch (ZipException e) {
                    e.printStackTrace();
                    System.out.println("文件解压失败：e = " + e);
                }
            } else {
                System.out.println("Zip文件不存在");
            }
            System.out.println("Zip文件解压成功");

            File ShapeDir = new File(bUploadPath + "/" + zipName + "/1.范围");

            if (!ShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }

            List<File> shapeFiles = new ArrayList<>();
            File[] childFiles = ShapeDir.listFiles();
            File tempFile = null;
            System.out.println("查找解压目录：" + ShapeDir + " 中 shape 文件");
            // 过滤目录中shape文件
            if (childFiles.length == 0) {
                typeList.add("1.范围文件夹中没有shp文件");
            } else {
                for (int i = 0; i < childFiles.length; i++) {
                    tempFile = childFiles[i];
                    if (tempFile.getName().endsWith(".shp")) {
                        shapeFiles.add(tempFile);
                    }
                }
            }
            System.out.println("shape 文件个数为 = " + shapeFiles.size());

            File dfShapeDir = new File(bUploadPath + "/" + zipName + "/2.堤防");

            if (!dfShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }

            List<File> dfshapeFiles = new ArrayList<>();
            File[] dfchildFiles = dfShapeDir.listFiles();
            File dftempFile = null;
            System.out.println("查找解压目录：" + dfShapeDir + " 中 shape 文件");
            // 过滤目录中shape文件
            if (dfchildFiles.length == 0) {
                typeList.add("2.堤防文件夹中没有shp文件");
            } else {
                for (int i = 0; i < dfchildFiles.length; i++) {
                    dftempFile = dfchildFiles[i];
                    if (dftempFile.getName().endsWith(".shp")) {
                        dfshapeFiles.add(dftempFile);
                    }
                }
            }
            System.out.println("shape 文件个数为 = " + dfshapeFiles.size());

            File szShapeDir = new File(bUploadPath + "/" + zipName + "/3.水闸");

            if (!szShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }

            List<File> szShapeFiles = new ArrayList<>();
            File[] szChildFiles = szShapeDir.listFiles();
            File szTempFile = null;
            System.out.println("查找解压目录：" + szShapeDir + " 中 shape 文件");
            // 过滤目录中shape文件
            if (szChildFiles.length == 0) {
                typeList.add("3.水闸文件夹中没有shp文件");
            } else {
                for (int i = 0; i < szChildFiles.length; i++) {
                    szTempFile = szChildFiles[i];
                    if (szTempFile.getName().endsWith(".shp")) {
                        szShapeFiles.add(szTempFile);
                    }
                }
            }
            System.out.println("shape 文件个数为 = " + szShapeFiles.size());

            File kmShapeDir = new File(bUploadPath + "/" + zipName + "/4.口门");

            if (!kmShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }

            List<File> kmShapeFiles = new ArrayList<>();
            File[] kmChildFiles = kmShapeDir.listFiles();
            File kmSempFile = null;
            System.out.println("查找解压目录：" + kmShapeDir + " 中 shape 文件");
            // 过滤目录中shape文件
            if (kmChildFiles.length == 0) {
                typeList.add("4.口门文件夹中没有shp文件");
            } else {
                for (int i = 0; i < kmChildFiles.length; i++) {
                    kmSempFile = kmChildFiles[i];
                    if (kmSempFile.getName().endsWith(".shp")) {
                        kmShapeFiles.add(kmSempFile);
                    }
                }
            }
            System.out.println("shape 文件个数为 = " + kmShapeFiles.size());

            File bzShapeDir = new File(bUploadPath + "/" + zipName + "/5.泵站");

            if (!bzShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }

            List<File> bzShapeFiles = new ArrayList<>();
            File[] bzChildFiles = bzShapeDir.listFiles();
            File bzSempFile = null;
            System.out.println("查找解压目录：" + bzShapeDir + " 中 shape 文件");
            // 过滤目录中shape文件
            if (bzChildFiles.length == 0) {
                typeList.add("5.泵站文件夹中没有shp文件");
            } else {
                for (int i = 0; i < bzChildFiles.length; i++) {
                    bzSempFile = bzChildFiles[i];
                    if (bzSempFile.getName().endsWith(".shp")) {
                        bzShapeFiles.add(bzSempFile);
                    }
                }
            }
            System.out.println("shape 文件个数为 = " + bzShapeFiles.size());

            File aqqShapeDir = new File(bUploadPath + "/" + zipName + "/6.安全区");

            if (!aqqShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }

            List<File> aqqShapeFiles = new ArrayList<>();
            File[] aqqChildFiles = aqqShapeDir.listFiles();
            File aqqSempFile = null;
            System.out.println("查找解压目录：" + aqqShapeDir + " 中 shape 文件");
            // 过滤目录中shape文件
            if (aqqChildFiles.length == 0) {
                typeList.add("6.安全区文件夹中没有shp文件");
            } else {
                for (int i = 0; i < aqqChildFiles.length; i++) {
                    aqqSempFile = aqqChildFiles[i];
                    if (aqqSempFile.getName().endsWith(".shp")) {
                        aqqShapeFiles.add(aqqSempFile);
                    }
                }
            }
            System.out.println("shape 文件个数为 = " + aqqShapeFiles.size());

            File aqtShapeDir = new File(bUploadPath + "/" + zipName + "/7.安全台");

            if (!aqtShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }

            List<File> aqtShapeFiles = new ArrayList<>();
            File[] aqtChildFiles = aqtShapeDir.listFiles();
            File aqtSempFile = null;
            System.out.println("查找解压目录：" + aqtShapeDir + " 中 shape 文件");
            // 过滤目录中shape文件
            if (aqtChildFiles.length == 0) {
                typeList.add("7.安全台文件夹中没有shp文件");
            } else {
                for (int i = 0; i < aqtChildFiles.length; i++) {
                    aqtSempFile = aqtChildFiles[i];
                    if (aqtSempFile.getName().endsWith(".shp")) {
                        aqqShapeFiles.add(aqtSempFile);
                    }
                }
            }
            System.out.println("shape 文件个数为 = " + aqqShapeFiles.size());

            File yjzdShapeDir = new File(bUploadPath + "/" + zipName + "/8.预警站点");

            if (!yjzdShapeDir.exists()) {
                System.out.println("shape目录不存在");
                return ReturnDTOUtil.error("Zip中shape目录不存在");
            }

            List<File> yjzdShapeFiles = new ArrayList<>();
            File[] yjzdChildFiles = yjzdShapeDir.listFiles();
            File yjzdSempFile = null;
            System.out.println("查找解压目录：" + yjzdShapeDir + " 中 shape 文件");
            // 过滤目录中shape文件
            if (yjzdChildFiles.length == 0) {
                typeList.add("8.预警站点文件夹中没有shp文件");
            } else {
                for (int i = 0; i < yjzdChildFiles.length; i++) {
                    yjzdSempFile = yjzdChildFiles[i];
                    if (yjzdSempFile.getName().endsWith(".shp")) {
                        yjzdShapeFiles.add(yjzdSempFile);
                    }
                }
            }
            System.out.println("shape 文件个数为 = " + yjzdShapeFiles.size());
        }

        if (typeList.size() == 0) {
            map.put("dacd", basinSpaceData.getDacd());
            map.put("danm", basinSpaceData.getDanm());
            map.put("content", basinSpaceData.getContent());
            map.put("deptName", basinSpaceData.getDeptName());
            map.put("num", 1);
            return uploadZipShapeParse(map);
        } else {
            map.put("type", 0);
            map.put("typeList", typeList);
            return ReturnDTOUtil.success(map);
        }
    }

    public void deleteFolder(File folder) {
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null && files.length > 0) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteFolder(file);
                    } else {
                        file.delete();
                    }
                }
            }
            //删除空文件夹
            folder.delete();
        } else {
            //如果是文件，直接删除
            folder.delete();
        }
    }
}

