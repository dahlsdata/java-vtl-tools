package no.ssb.vtl.rest;

/*
 * -
 *  * ========================LICENSE_START=================================
 *  * Java VTL
 *  * %%
 *  * Copyright (C) 2017 Arild Johan Takvam-Borge
 *  * %%
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  * =========================LICENSE_END==================================
 *
 */

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import no.ssb.vtl.connectors.spring.RestTemplateConnector;
import no.ssb.vtl.connectors.spring.converters.DataStructureHttpConverter;
import no.ssb.vtl.connectors.spring.converters.DatasetHttpMessageConverter;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.script.VTLScriptEngine;
import org.assertj.core.api.SoftAssertions;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.mock.http.MockHttpInputMessage;
import org.springframework.web.client.RestTemplate;

import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Arrays.*;

public class IntegrationTests {

    @Test
    @Ignore
    public void testHierarchyResult() throws ScriptException, IOException {
        SoftAssertions softly = new SoftAssertions();

        String pathT1 = "summed_down_to_region_and_period.json";
        String pathT2 = "hierarki_kommuneregion.json";

        ObjectMapper mapper = new ObjectMapper();
        DatasetHttpMessageConverter datasetConverter = new DatasetHttpMessageConverter(mapper);

        DataStructureHttpConverter structureConverter = new DataStructureHttpConverter(mapper);

        Dataset t1 = getDatasetFromFile(datasetConverter, pathT1);
        Dataset t2 = getDatasetFromFile(datasetConverter, pathT2);

        List<String> expectedResult = calculateResult(getDatasetFromFile(datasetConverter, pathT1), getDatasetFromFile(datasetConverter, pathT2));

        RestTemplate restTemplate = getDatasetFromConnector(asList(structureConverter, datasetConverter));

        RestTemplateConnector connector = new RestTemplateConnector(restTemplate, Executors.newCachedThreadPool());
        VTLScriptEngine engine = new VTLScriptEngine(connector);

        SimpleBindings bindings = new SimpleBindings();
        bindings.put("t1", t1);
        bindings.put("t2", t2);

//        List<String> expectedResult = new ArrayList<>();
//
//        expectedResult.add("-304835.0");
//        expectedResult.add("-159553.0");
//        expectedResult.add("-304835.0");
//        expectedResult.add("-54428.0");
//        expectedResult.add("9120.0");
//        expectedResult.add("-152234.0");
//        expectedResult.add("512.0");
//        expectedResult.add("46628.0");
//        expectedResult.add("25012.0");
//        expectedResult.add("-18673.0");
//        expectedResult.add("-38807.0");
//        expectedResult.add("10743.0");
//        expectedResult.add("-162494.0");
//        expectedResult.add("8586.0");
//        expectedResult.add("71049.0");
//        expectedResult.add("-16920.0");
//        expectedResult.add("9120.0");
//        expectedResult.add("-162494.0");
//        expectedResult.add("-162494.0");
//        expectedResult.add("3118.0");
//        expectedResult.add("-144867.0");
//        expectedResult.add("-32938.0");
//        expectedResult.add("-152234.0");
//        expectedResult.add("3118.0");
//        expectedResult.add("28410.0");
//        expectedResult.add("-5869.0");
//        expectedResult.add("17627.0");

        engine.eval(getHierarchyExpression(), bindings);
        Dataset result = (Dataset) bindings.get("resultat");


        List<String> resultList = result.getData().map(row -> row.get(2).get().toString()).collect(Collectors.toList());

        expectedResult.forEach(r -> softly.assertThat(resultList).contains(r));
        softly.assertAll();
    }

    private List<String> calculateResult(Dataset sums, Dataset hierarchy) {
        List<DataPoint> sumData = sums.getData().collect(Collectors.toList());
        List<DataPoint> hierarchyData = hierarchy.getData()
                .map(row -> new DataPoint(row.get(0), row.get(1), row.get(2)))
                .filter(row -> row.get(1).toString().equals("2016")).collect(Collectors.toList());

        //Get all hierarchies
        Collection<String> values = hierarchyData.stream()
                .map(row -> row.get(2))
                .collect(Collectors.toMap(row -> row.get().toString(), row -> row.get().toString(), (row, v) -> row)).values();

        List<String> result = new ArrayList<>();

        values.forEach( value -> {
            // Get all regions for To-value
            List<String> regions = hierarchyData.stream()
                    .filter(row -> row.get(2).toString().equals(value))
                    .map(row -> row.get(0).get().toString())
                    .collect(Collectors.toList());

//            List<DataPoint> collect = sumData.stream().filter(row -> regions.contains(row.get(1).get().toString())).collect(Collectors.toList());

            //get sum of all regions for this To-value
            result.add("" + sumData.stream()
                    .filter(row -> regions.contains(row.get(1).get().toString()))
                    .mapToDouble(row -> Double.parseDouble(row.get(2).get().toString()))
                    .sum());
                }
        );

        return result.stream().filter(v -> !v.equals("0.0"))
                .collect(Collectors.toList());
    }

    @Test
    @Ignore
    public void testUnionSpeed() throws IOException, ScriptException {

        ObjectMapper mapper = new ObjectMapper();
        DatasetHttpMessageConverter datasetConverter = new DatasetHttpMessageConverter(mapper);

        DataStructureHttpConverter structureConverter = new DataStructureHttpConverter(mapper);

        Dataset t1 = getDatasetFromFile(datasetConverter, "0A_2016.json");
        Dataset t2 = getDatasetFromFile(datasetConverter, "0C_2016.json");

        RestTemplate restTemplate = getDatasetFromConnector(asList(structureConverter, datasetConverter));

        RestTemplateConnector connector = new RestTemplateConnector(restTemplate, Executors.newCachedThreadPool());
        VTLScriptEngine engine = new VTLScriptEngine(connector);

        MetricRegistry registry = new MetricRegistry();
        ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).build();

        SimpleBindings bindings = new SimpleBindings();
        bindings.put("t1", t1);
        bindings.put("t2", t2);


        Meter rows = registry.meter("rows");
        Meter cells = registry.meter("cells");
        Timer timerEvaluation = registry.timer("evaluation-time");
        Timer timerExecution = registry.timer("execution-time");
        try (Timer.Context timeEv = timerEvaluation.time()) {
            Object eval = engine.eval(getUnionExpression(), bindings);
            Dataset result = (Dataset) bindings.get("resultat");
            timeEv.stop();
            try (Timer.Context timeEx = timerExecution.time()) {
                result.getData().forEach(data -> {
                    rows.mark();
                    data.forEach(cell -> cells.mark());
                });
                timeEx.stop();
            }
        }

        reporter.report();
    }

    private RestTemplate getDatasetFromConnector(List<HttpMessageConverter<?>> converters) {
        RestTemplate template = new RestTemplate();
        template.setMessageConverters(converters);

//        MockRestServiceServer server = MockRestServiceServer.createServer(template);
//        server.expect(requestTo("http://t1")).andRespond(
//                withSuccess(
//                        new FileSystemResource("/Users/hadrien/Downloads/0C_2016.csv"),
//                        MediaType.valueOf("application/ssb.dataset+json")
//                )
//        );
//
//        server.expect(requestTo("http://t1")).andRespond(
//                withSuccess(
//                        new FileSystemResource("/Users/hadrien/Downloads/0C_2016.csv"),
//                        MediaType.valueOf("application/ssb.dataset+json")
//                )
//        );

        return template;
    }

    private Dataset getDatasetFromFile(DatasetHttpMessageConverter converter, String fileName) throws IOException {
        ByteSource file = Files.asByteSource(new File("src/test/resources/" + fileName));
        return (Dataset) converter.read(Dataset.class, new MockHttpInputMessage(
                file.openBufferedStream()
        ));
    }

    private String getUnionExpression() {
        return "" +
                "komm := [t1]{\n" +
                "    rename t1.PERIODE to periode,\n" +
                "    identifier fylkesregion := if t1.BYDEL is not null and t1.BYDEL <> \"00\" then t1.REGION || t1.BYDEL else t1.REGION,\n" +
                "    identifier kontoklasse := t1.KONTOKLASSE, \n" +
                "    identifier funksjon := t1.FUNKSJON_KAPITTEL, \n" +
                "    identifier art := t1.ART_SEKTOR, \n" +
                "\n" +
                "    measure belop := if integer_from_string(art) < 600 then t1.BELOP * 1.0 else t1.BELOP * -1.0,\n" +
                "\n" +
                "    filter periode is not null and fylkesregion is not null and kontoklasse is not null and funksjon is not null and art is not null and belop is not null,\n" +
                "    \n" +
                "    keep periode, fylkesregion, kontoklasse, funksjon, art, belop\n" +
                "}\n" +
                "\n" +
                "resultat_komm := sum(komm.belop) group by periode, fylkesregion, kontoklasse, funksjon, art\n" +
                "\n" +
                "fykomm := [t2]{ \n" +
                "    rename t2.PERIODE to periode,\n" +
                "    identifier fylkesregion := t2.REGION,\n" +
                "    identifier kontoklasse := t2.KONTOKLASSE, \n" +
                "    identifier funksjon := t2.FUNKSJON_KAPITTEL, \n" +
                "    identifier art := t2.ART_SEKTOR, \n" +
                "\n" +
                "    measure belop := if integer_from_string(art) < 600 then t2.BELOP * 1.0 else t2.BELOP * -1.0,\n" +
                "    \n" +
                "    filter periode is not null and fylkesregion is not null and kontoklasse is not null and funksjon is not null and art is not null and belop is not null,\n" +
                "\n" +
                "    keep periode, fylkesregion, kontoklasse, funksjon, art, belop\n" +
                "}\n" +
                "\n" +
                "resultat_fykomm := sum(fykomm.belop) group by periode, fylkesregion, kontoklasse, funksjon, art\n" +
                "\n" +
                "resultat := union(resultat_komm, resultat_fykomm)";
    }

    private String getHierarchyExpression() {
        return "resultat := hierarchy (t1, t1.REGION, t2, true)";
    }
}
