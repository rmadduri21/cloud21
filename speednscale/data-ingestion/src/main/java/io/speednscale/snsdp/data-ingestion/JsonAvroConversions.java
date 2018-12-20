package io.speednscale.snsdp.data_ingestion;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.util.Collection;
import java.util.Iterator;

import javax.inject.Inject;

/*import play.libs.ws.*;
import play.libs.F.Promise;
import play.libs.Json;*/

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import io.speednscale.snsdp.snsdp_record_models.raw.game_telemetry;
import io.speednscale.snsdp.snsdp_record_models.raw.data;
import io.speednscale.snsdp.snsdp_record_models.raw._mp_server_metadata;
import io.speednscale.snsdp.snsdp_record_models.raw.fyber_report;
import io.speednscale.snsdp.snsdp_record_models.raw.adjust_postback;
import io.speednscale.snsdp.snsdp_record_models.raw.event_v_1_0;

public class JsonAvroConversions {
	
	public static byte[] jsonToNakedAvro(String json, org.apache.avro.Schema schema) throws IOException {
		
	    InputStream input = null;
	    GenericDatumWriter<GenericRecord> writer = null;
	    Encoder encoder = null;
	    ByteArrayOutputStream output = null;
	    try {
	        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
	        input = new ByteArrayInputStream(json.getBytes());
	        output = new ByteArrayOutputStream();
	        DataInputStream din = new DataInputStream(input);
	        writer = new GenericDatumWriter<GenericRecord>(schema);
	        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
	        encoder = EncoderFactory.get().binaryEncoder(output, null);
	        GenericRecord datum;
	        while (true) {
	            try {
	                datum = reader.read(null, decoder);
	            } catch (EOFException eofe) {
	                break;
	            }
	            writer.write(datum, encoder);
	        }
	        encoder.flush();
	        return output.toByteArray();
	    } finally {
	        try { input.close(); } catch (Exception e) { }
	    }
	}
	
	public static String nakedAvroToJson(byte[] avro, Schema schema, boolean pretty) throws IOException {
		
	    GenericDatumReader<GenericRecord> reader = null;
	    JsonEncoder encoder = null;
	    ByteArrayOutputStream output = null;
	    try {
	        reader = new GenericDatumReader<GenericRecord>(schema);
	        InputStream input = new ByteArrayInputStream(avro);
	        output = new ByteArrayOutputStream();
	        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
	        encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
	        Decoder decoder = DecoderFactory.get().binaryDecoder(input, null);
	        GenericRecord datum;
	        while (true) {
	            try {
	                datum = reader.read(null, decoder);
	            } catch (EOFException eofe) {
	                break;
	            }
	            writer.write(datum, encoder);
	        }
	        encoder.flush();
	        output.flush();
	        return new String(output.toByteArray());
	    } finally {
	        try { if (output != null) output.close(); } catch (Exception e) { }
	    }
	}
	
	public static byte[] jsonToAvro(String json, Schema schema) throws IOException {
	    InputStream input = null;
	    DataFileWriter<GenericRecord> writer = null;
	    Encoder encoder = null;
	    ByteArrayOutputStream output = null;
	    try {
	        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
	        input = new ByteArrayInputStream(json.getBytes());
	        output = new ByteArrayOutputStream();
	        DataInputStream din = new DataInputStream(input);
	        writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());
	        writer.create(schema, output);
	        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
	        GenericRecord datum;
	        while (true) {
	            try {
	                datum = reader.read(null, decoder);
	            } catch (EOFException eofe) {
	                break;
	            }
	            writer.append(datum);
	        }
	        writer.flush();
	        return output.toByteArray();
	    } finally {
	        try { input.close(); } catch (Exception e) { }
	    }
	}
	
	public static String avroToJson(byte[] avro, boolean pretty) throws IOException {
		
	    GenericDatumReader<GenericRecord> reader = null;
	    JsonEncoder encoder = null;
	    ByteArrayOutputStream output = null;
	    try {
	        reader = new GenericDatumReader<GenericRecord>();
	        InputStream input = new ByteArrayInputStream(avro);
	        DataFileStream<GenericRecord> streamReader = new DataFileStream<GenericRecord>(input, reader);
	        output = new ByteArrayOutputStream();
	        Schema schema = streamReader.getSchema();
	        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
	        encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
	        for (GenericRecord datum : streamReader) {
	            writer.write(datum, encoder);
	        }
	        encoder.flush();
	        output.flush();
	        return new String(output.toByteArray());
	    } finally {
	        try { if (output != null) output.close(); } catch (Exception e) { }
	    }
	}
	
	public static GenericRecord createBlankAvroRecordForSchema(Schema schema) {
		Record record = null;
	    try {
			record = new Record(schema);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return record;
	}
	
	public static byte[] avroRecordToNakedAvroBytes(GenericRecord record, Schema schema) {
		
	    byte[] outputBytes = null;
	    try {
	        ByteArrayOutputStream output = new ByteArrayOutputStream();
	        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
	        Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
	        writer.write(record, encoder);
	        encoder.flush();
	        outputBytes = output.toByteArray();
		}
		catch(Exception e) {
			e.printStackTrace();
			outputBytes = null;
		}
		return outputBytes;
	}
	
	public static void setDefaultValuesForBlankAvroRecord(GenericRecord avroRecord) {
		// recursive
		try {
			List<Schema.Field> fields = avroRecord.getSchema().getFields();
			for(Schema.Field field : fields) {
				String fieldName = field.name();
				org.apache.avro.Schema.Type fieldType = field.schema().getType();
				switch(fieldType) {
					case ARRAY:
						avroRecord.put(fieldName, new ArrayList());
						break;
					case BOOLEAN:
						avroRecord.put(fieldName, false);
						break;
					case BYTES:
						avroRecord.put(fieldName, new byte[0]);
						break;
					case DOUBLE:
						avroRecord.put(fieldName, 0.0d);
						break;
					case ENUM:
						avroRecord.put(fieldName, 1); // this will probably break
						break;
					case FIXED:
						avroRecord.put(fieldName, 0.0f); // this will probably break
						break;
					case FLOAT:
						avroRecord.put(fieldName, 0.0f);
						break;
					case INT:
						avroRecord.put(fieldName, 0);
						break;
					case LONG:
						avroRecord.put(fieldName, 0L);
						break;
					case MAP:
						Map map = new HashMap();
						avroRecord.put(fieldName, map);
						break;
					case NULL:
						avroRecord.put(fieldName, null); // this may break
						break;
					case RECORD:
						try {
							Record nestedRecord = new Record(avroRecord.getSchema().getField(fieldName).schema());
							setDefaultValuesForBlankAvroRecord(nestedRecord); // this'll have to be recursive
							avroRecord.put(fieldName, nestedRecord);
						}
						catch(Exception ee) {
							ee.printStackTrace();
						}
						break;
					case STRING:
						avroRecord.put(fieldName, "");
						break;
					case UNION:
						try {
							// org.apache.avro.Schema.Type[] types = field.schema().getTypes();
							// do something clever with digging out types[0] and doing what's appropriate for that;
							avroRecord.put(fieldName, ""); // this most likely will break
						}
						catch(Exception ee) {
							ee.printStackTrace();
						}
						break;
					default:
						// do nothing
						break;
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static void populateAvroRecordFieldOrMapEntryFromFlatJson(String jsonFieldName, JsonNode jsonObject, GenericRecord avroRecord, Map map, String mapFieldPrefix) {
		try {
			if(jsonObject.has(jsonFieldName)) {
				JsonNode jsonNode = jsonObject.get(jsonFieldName);
				if(jsonNode != null) {
					// does avroRecord have corresponding fieldName?
					String avroFieldName = jsonFieldName.toLowerCase().replaceAll("-","_");
					if(avroFieldName.startsWith("_")) {
						avroFieldName = avroFieldName.substring(1,avroFieldName.length());
					}
					org.apache.avro.Schema.Field avroField = avroRecord.getSchema().getField(avroFieldName);
					if(avroField != null) {
						org.apache.avro.Schema.Type avroFieldType = avroField.schema().getType();
						switch(avroFieldType) {
							// HANDLE THE RECORD CASE -- RECURSION!!!!
							// HANDLE THE RECORD CASE -- RECURSION!!!!
							// HANDLE OTHER TYPES, TOO -- ARRAYS, ENUMS, ETC!!!
							// HANDLE OTHER TYPES, TOO!!!
							// HANDLE OTHER TYPES, TOO!!!
							case STRING:
								if (jsonNode.textValue() != null)
									avroRecord.put(avroFieldName, jsonNode.textValue());
								break;
							case INT:
								avroRecord.put(avroFieldName, jsonNode.intValue());
								break;
							case LONG:
								avroRecord.put(avroFieldName, jsonNode.longValue());
								break;
							case BOOLEAN:
								try {
									avroRecord.put(avroFieldName, jsonNode.booleanValue());
								}
								catch(Exception eignored) {
									try {
										int boolFlag = -1;
										boolFlag = jsonNode.intValue();
										switch(boolFlag) {
											case 0:
												avroRecord.put(avroFieldName, false);
												break;
											case 1:
												avroRecord.put(avroFieldName, true);
												break;
											default:
												break;
										}
									}
									catch(Exception eeignored) {
										// ignored
									}
								}
								break;
							default:
								avroRecord.put(avroFieldName, jsonNode.asText());
								break;
						}
					}
					else {
						// HANDLE MULTIPLE DATA TYPES IN MAPS MORE ELEGANTLY
						map.put("" + mapFieldPrefix + "__" + avroFieldName, jsonNode.asText());
					}
				}
			}
		}
		catch(Exception e) {
			System.out.println("BrainDeadAvroConverter.populateAvroRecordFieldOrMapEntryFromFlatJson() : Exception : " + e.getMessage());
		}
	}
	
	public static void populateAvroRecordOrMapFromFlatJson(JsonNode jsonObject, GenericRecord avroRecord, Map map, String mapFieldPrefix) {
		// make it recursive
		// make it recursive
		// make it recursive
		try {
			Iterator<String> jsonFieldNames = jsonObject.fieldNames();
			while (jsonFieldNames.hasNext()) {
				populateAvroRecordFieldOrMapEntryFromFlatJson(jsonFieldNames.next(), jsonObject, avroRecord, map, mapFieldPrefix);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static byte[] getGameTelemetryAvroBytesFromJson(String jsonString) {
		byte[] avroBytes = null;
		
		
		try {
		    ObjectMapper mapper = new ObjectMapper();
		    JsonNode jsonObject = mapper.readTree(jsonString);
			
			GenericRecord record = createBlankAvroRecordForSchema(game_telemetry.getClassSchema());
			
			// add top level fields
			if(jsonObject.has("RecordName")) {
				record.put("recordname", jsonObject.get("RecordName").textValue());
			}
			if(jsonObject.has("game")) {
				record.put("game", jsonObject.get("game").textValue());
			}
			
			// extendedDataFields Map
			Map<String, String> extendedDataFieldsMap = new HashMap<String, String>();
			// extendedDataFieldsMap.put("test_added_field", "test_added_value");
			
			// add data object
			if(jsonObject.has("Data")) {
				JsonNode jsonDataObject = jsonObject.get("Data");
				GenericRecord dataRecord = createBlankAvroRecordForSchema(data.getClassSchema());
				setDefaultValuesForBlankAvroRecord(dataRecord);
				populateAvroRecordOrMapFromFlatJson(jsonDataObject, dataRecord, extendedDataFieldsMap, "data");
				record.put("data",dataRecord);
			}
			
			// add mp_server_metadata object
			if(jsonObject.has("_mp_server_metadata")) {
				JsonNode jsonMpServerMetadataObject = jsonObject.get("_mp_server_metadata");
				GenericRecord mpServerMetadataRecord = createBlankAvroRecordForSchema(_mp_server_metadata.getClassSchema());
				setDefaultValuesForBlankAvroRecord(mpServerMetadataRecord);
				populateAvroRecordOrMapFromFlatJson(jsonMpServerMetadataObject, mpServerMetadataRecord, extendedDataFieldsMap, "mp_server_metadata");
				record.put("_mp_server_metadata", mpServerMetadataRecord);
			}
			
			// add extended_data_fields Map to avroRecord
			record.put("extended_data_fields",extendedDataFieldsMap);
			
			// write it out as bytes
			avroBytes = avroRecordToNakedAvroBytes(record, game_telemetry.getClassSchema());
		}
		catch(Exception e) {
			e.printStackTrace();
			avroBytes = null;
		}
		return avroBytes;
	}
	public static byte[] getAdjustPostbackAvroBytesFromJson(String jsonString) {
		byte[] avroBytes = null;
		try {
		    ObjectMapper mapper = new ObjectMapper();
		    JsonNode jsonObject = mapper.readTree(jsonString);
			
			GenericRecord record = createBlankAvroRecordForSchema(adjust_postback.getClassSchema());
			
			// because adjust_postback is a single-level avro/json object with most fields in the top level,
			// instead of a "data" object inside, we have to call setDefaultValuesForBlankAvroRecord() 
			// BEFORE setting any of those top level fields explicitly, 
			// to avoid subsequently zeroing them out again when setDefaultValuesForBlankAvroRecord() called
			setDefaultValuesForBlankAvroRecord(record);

			// add top level fields
			if(jsonObject.has("action")) {
				record.put("recordname", jsonObject.get("action").textValue());
			}
			if(jsonObject.has("game")) {
				record.put("game", jsonObject.get("game").textValue());
			}
			
			// extendedDataFields Map
			Map<String, String> extendedDataFieldsMap = new HashMap<String, String>();
			// extendedDataFieldsMap.put("test_added_field", "test_added_value");
			
			populateAvroRecordOrMapFromFlatJson(jsonObject, record, extendedDataFieldsMap, "adjust_postback");
			
			// add mp_server_metadata object
			if(jsonObject.has("_mp_server_metadata")) {
				JsonNode jsonMpServerMetadataObject = jsonObject.get("_mp_server_metadata");
				GenericRecord mpServerMetadataRecord = createBlankAvroRecordForSchema(_mp_server_metadata.getClassSchema());
				setDefaultValuesForBlankAvroRecord(mpServerMetadataRecord);
				populateAvroRecordOrMapFromFlatJson(jsonMpServerMetadataObject, mpServerMetadataRecord, extendedDataFieldsMap, "mp_server_metadata");
				record.put("_mp_server_metadata", mpServerMetadataRecord);
			}
			
			// add extended_data_fields Map to avroRecord
			record.put("extended_data_fields",extendedDataFieldsMap);
			
			// write it out as bytes
			avroBytes = avroRecordToNakedAvroBytes(record, adjust_postback.getClassSchema());
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return avroBytes;
	}

	public static byte[] getFyberReportAvroBytesFromJson(String jsonString) {
		byte[] avroBytes = null;
		try {
		    ObjectMapper mapper = new ObjectMapper();
		    JsonNode jsonObject = mapper.readTree(jsonString);
			
			GenericRecord record = createBlankAvroRecordForSchema(fyber_report.getClassSchema());
			
			// because fyber_report is a single-level avro/json object with most fields in the top level,
			// instead of a "data" object inside, we have to call setDefaultValuesForBlankAvroRecord() 
			// BEFORE setting any of those top level fields explicitly, 
			// to avoid subsequently zeroing them out again when setDefaultValuesForBlankAvroRecord() called
			setDefaultValuesForBlankAvroRecord(record);

			// add top level fields
			if(jsonObject.has("action")) {
				record.put("recordname", jsonObject.get("action").textValue());
			}
			if(jsonObject.has("game")) {
				record.put("game", jsonObject.get("game").textValue());
			}
			
			// extendedDataFields Map
			Map<String, String> extendedDataFieldsMap = new HashMap<String, String>();
			// extendedDataFieldsMap.put("test_added_field", "test_added_value");
			
			populateAvroRecordOrMapFromFlatJson(jsonObject, record, extendedDataFieldsMap, "fyber_report");
			
			// add mp_server_metadata object
			if(jsonObject.has("_mp_server_metadata")) {
				JsonNode jsonMpServerMetadataObject = jsonObject.get("_mp_server_metadata");
				GenericRecord mpServerMetadataRecord = createBlankAvroRecordForSchema(_mp_server_metadata.getClassSchema());
				setDefaultValuesForBlankAvroRecord(mpServerMetadataRecord);
				populateAvroRecordOrMapFromFlatJson(jsonMpServerMetadataObject, mpServerMetadataRecord, extendedDataFieldsMap, "mp_server_metadata");
				record.put("_mp_server_metadata", mpServerMetadataRecord);
			}
			
			// add extended_data_fields Map to avroRecord
			record.put("extended_data_fields",extendedDataFieldsMap);
			
			// write it out as bytes
			avroBytes = avroRecordToNakedAvroBytes(record, fyber_report.getClassSchema());
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return avroBytes;
	}
    
	// telemetry event_v_1_0
	public static event_v_1_0 getTelemetryEventObjectFromAvroBytes(byte[] telemetryEventBytes) {
		DatumReader<event_v_1_0> reader=new SpecificDatumReader<event_v_1_0>(event_v_1_0.class);
		Decoder decoder=DecoderFactory.defaultFactory().createBinaryDecoder(telemetryEventBytes,null);
		event_v_1_0 telemetryEventObject = null;
		try {
			telemetryEventObject = reader.read(null,decoder);
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return telemetryEventObject;
	}
	
	// game_telemetry
	public static game_telemetry getGameTelObjectFromAvroBytes(byte[] gameTelAvroBytes) {
		DatumReader<game_telemetry> reader=new SpecificDatumReader<game_telemetry>(game_telemetry.class);
		Decoder decoder=DecoderFactory.defaultFactory().createBinaryDecoder(gameTelAvroBytes,null);
		game_telemetry gameTelObject = null;
		try {
			gameTelObject = reader.read(null,decoder);
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return gameTelObject;
	}
	
	// fyber_report
	public static fyber_report getFyberReportObjectFromAvroBytes(byte[] fyberReportAvroBytes) {
		DatumReader<fyber_report> reader=new SpecificDatumReader<fyber_report>(fyber_report.class);
		Decoder decoder=DecoderFactory.defaultFactory().createBinaryDecoder(fyberReportAvroBytes,null);
		fyber_report fyberReportObject = null;
		try {
			fyberReportObject = reader.read(null,decoder);
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return fyberReportObject;
	}
	
	// adjust_postback
	public static adjust_postback getAdjustPostbackObjectFromAvroBytes(byte[] adjustPostbackAvroBytes) {
		DatumReader<adjust_postback> reader=new SpecificDatumReader<adjust_postback>(adjust_postback.class);
		Decoder decoder=DecoderFactory.defaultFactory().createBinaryDecoder(adjustPostbackAvroBytes,null);
		adjust_postback adjustPostbackObject = null;
		try {
			adjustPostbackObject = reader.read(null,decoder);
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return adjustPostbackObject;
	}
	
	// event_v_1_0
	public static event_v_1_0 getEventObjectFromAvroBytes(byte[] eventBytes) {
		DatumReader<event_v_1_0> reader=new SpecificDatumReader<event_v_1_0>(event_v_1_0.class);
		Decoder decoder=DecoderFactory.defaultFactory().createBinaryDecoder(eventBytes,null);
		event_v_1_0 eventObject = null;
		try {
			eventObject = reader.read(null,decoder);
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return eventObject;
	}
	
	public static void main(String[] args) {
		
/*		try {
			
			String jsonRecord = BrainDeadFileLogger.readFromFile("../avro/avrotest.json");
			String avroSchema = BrainDeadFileLogger.readFromFile("../avro/avrotest.avsc");
			byte[] avroRecord = BrainDeadAvroConverter.jsonToAvro(jsonRecord, avroSchema);
			System.out.println(">>> AVRO Record : " + avroRecord.length + " bytes");
			System.out.println(">>> Reconstituted JSON Record : ");
			System.out.println(BrainDeadAvroConverter.avroToJson(avroRecord, true)); 

			BrainDeadAvroConverter.setAvroSchemasDirectoryPathIfNeeded("../avro");
			BrainDeadAvroConverter.reloadAllSchemasIfNeeded();
			
			String jsonString = BrainDeadFileLogger.readFromFile("../avro/avrotest.json");
			String schemaString = avroSchemas.get("avrotest");
			GenericRecord record = createBlankAvroRecordForSchema(schemaString);
			record.put("name", "Joe");
			record.put("age", 105);
			byte[] avroBytes = avroRecordToNakedAvroBytes(record, schemaString);
			System.out.println(">>> AVRO Record : " + avroBytes.length + " bytes");
			System.out.println(">>> Reconstituted JSON Record : ");
			System.out.println(BrainDeadAvroConverter.nakedAvroToJson(avroBytes, schemaString, true)); 

			String gameTelemetryJsonString = BrainDeadFileLogger.readFromFile("../avro/current_game_telemetry_sample.json");
			byte[] gameTelemetryAvroBytes = getGameTelemetryAvroBytesFromJson(gameTelemetryJsonString);
			System.out.println(">>> GameTelemetry AVRO Record : " + gameTelemetryAvroBytes.length + " bytes");
			System.out.println(">>> Reconstituted JSON Record : ");
			String gameTelemetrySchemaString = BrainDeadAvroConverter.avroSchemas.get("game_telemetry");
			System.out.println(BrainDeadAvroConverter.nakedAvroToJson(gameTelemetryAvroBytes, gameTelemetrySchemaString, true)); 
			
			String adjustPostbackJsonString = BrainDeadFileLogger.readFromFile("../avro/current_adjust_postback_sample.json");
			byte[] adjustPostbackAvroBytes = getdAdjustPostbackAvroBytesFromJson(adjustPostbackJsonString);
			System.out.println(">>> AdjustPostback AVRO Record : " + adjustPostbackAvroBytes.length + " bytes");
			System.out.println(">>> Reconstituted JSON Record : ");
			String adjustPostbackSchemaString = BrainDeadAvroConverter.avroSchemas.get("adjust_postback");
			System.out.println(BrainDeadAvroConverter.nakedAvroToJson(adjustPostbackAvroBytes, adjustPostbackSchemaString, true)); 

			String fyberReportJsonString = BrainDeadFileLogger.readFromFile("../avro/current_fyber_report_sample.json");
			byte[] fyberReportAvroBytes = getdFyberReportAvroBytesFromJson(fyberReportJsonString);
			System.out.println(">>> FyberReport AVRO Record : " + fyberReportAvroBytes.length + " bytes");
			System.out.println(">>> Reconstituted JSON Record : ");
			String fyberReportSchemaString = BrainDeadAvroConverter.avroSchemas.get("fyber_report");
			System.out.println(BrainDeadAvroConverter.nakedAvroToJson(fyberReportAvroBytes, fyberReportSchemaString, true)); 

			
		  // Record record=new Record(Person.SCHEMA$);
		  // record.put("name","John Doe");
		  // record.put("age",42);
		  // record.put("siblingnames",Lists.newArrayList());

		}
		catch(Exception e) {
			e.printStackTrace();
		}*/
	}	
}
