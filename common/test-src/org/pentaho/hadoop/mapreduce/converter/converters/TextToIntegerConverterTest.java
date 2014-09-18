/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.hadoop.mapreduce.converter.converters;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.pentaho.hadoop.mapreduce.converter.TypeConversionException;

import static org.junit.Assert.*;

public class TextToIntegerConverterTest {
  @Test
  public void canConvert() throws Exception {
    TextToIntegerConverter c = new TextToIntegerConverter();

    assertTrue( c.canConvert( Text.class, Integer.class ) );
    assertFalse( c.canConvert( Integer.class, Text.class ) );
    assertFalse( c.canConvert( null, null ) );
    assertFalse( c.canConvert( Text.class, Object.class ) );
    assertFalse( c.canConvert( Object.class, Integer.class ) );
  }

  @Test
  public void convert() throws Exception {
    TextToIntegerConverter c = new TextToIntegerConverter();
    Integer expected = 100;

    assertEquals( expected, c.convert( null, new Text( String.valueOf( expected ) ) ) );

    try {
      c.convert( null, null );
      fail();
    } catch ( NullPointerException ex ) {
      // Expected
    }

    try {
      c.convert( null, new Text( "not an integer" ) );
    } catch ( TypeConversionException ex ) {
      assertTrue( ex.getMessage().contains( "Error converting to" ) );
    }

  }
}
