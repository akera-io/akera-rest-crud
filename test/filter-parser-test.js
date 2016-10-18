var should = require('should');
var FilterParser = require('../src/filter-parser.js');

describe(
  'Filter conversion',
  function() {

    it(
      'should parse kendo simple filter',
      function() {

        var filter = FilterParser
          .convert('{"operator": "eq", "value": 5, "field": "CustNum"}');
        should(filter).have.property('CustNum', 5);

        filter = FilterParser
          .convert('[{"operator": "eq", "value": 5, "field": "CustNum"}]');
        should(filter).have.property('CustNum', 5);

        filter = FilterParser
          .convert('{"filters": [{"operator": "eq", "value": 5, "field": "CustNum"}]}');
        should(filter).have.property('CustNum', 5);

        filter = FilterParser
          .convert('{"logic": "and", "filters": [ { "operator": "eq", "value": 5, "field": "CustNum"}, { "operator": "gt", "value": 34.5, "field": "Discount"}]}');
        should(filter).have.property('and');
        should(filter.and[0]).have.property('CustNum', 5);
        should(filter.and[1]).have.property('Discount');
      });

    it(
      'should parse rollbase simple filter',
      function() {

        should(FilterParser.convert("State='AK'")).have.property('State', 'AK');
        should(FilterParser.convert("State=true")).have.property('State', true);
        should(FilterParser.convert("State=false")).have.property('State',
          false);
        should(FilterParser.convert("State=234.45")).have.property('State',
          234.45);
        should(FilterParser.convert("State=25")).have.property('State', 25);
        
        should(FilterParser.convert("isoIdt=1000 AND punchNum=2")).have.property('and');
      });

    it(
      'should parse rollbase simple group filter',
      function() {

        var filter = FilterParser
          .convert("State BEGINS 'a' OR Region MATCHES '*st'");

        should(filter).have.property('or');
        should(filter.or[0]).have.property('State');
        should(filter.or[1]).have.property('Region');

        filter = FilterParser
          .convert("{\"ablFilter\":\"Region eq 'West' AND State BEGINS 'c'\",\"skip\":0,\"top\":20}");

        should(filter).have.property('and');
        should(filter.and[0]).have.property('Region', 'West');
        should(filter.and[1]).have.property('State');

        filter = FilterParser
          .convert('{"ablFilter":"Region=\'West\' AND State BEGINS \'a\'"}');
        
        should(filter).have.property('and');
        should(filter.and[0]).have.property('Region', 'West');
        should(filter.and[1]).have.property('State');
      });

    it(
      'should parse rollbase complex group filter',
      function() {

        var filter = FilterParser
          .convert("(State = 'a' OR Region MATCHES '*st') and (city > 3 or country = 'cluj')");

        should(filter).have.property('and');
        should(filter.and[0]).have.property('or');
        should(filter.and[1]).have.property('or');

        should(filter.and[0].or[0]).have.property('State', 'a');
        should(filter.and[1].or[1]).have.property('country', 'cluj');
      });
  });
