package encoders

import (
	"github.com/elliotchance/orderedmap/v3"
	"github.com/fbz-tec/pgxport/core/formatters"
	"gopkg.in/yaml.v3"
)

type OrderedYamlEncoder struct {
	timeLayout string
	timezone   string
}

func NewOrderedYamlEncoder(timeFormat, timeZone string) OrderedYamlEncoder {
	return OrderedYamlEncoder{
		timeLayout: timeFormat,
		timezone:   timeZone,
	}
}

// EncodeRow builds a YAML mapping node (one record).
func (o OrderedYamlEncoder) EncodeRow(rowData *orderedmap.OrderedMap[string, DataParams]) (*yaml.Node, error) {

	row := &yaml.Node{
		Kind: yaml.MappingNode,
	}

	for k, v := range rowData.AllFromFront() {
		keyNode := &yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: k,
		}

		val := formatters.FormatYAMLValue(v.Value, v.ValueType, o.timeLayout, o.timezone)
		valueNode := &yaml.Node{}
		if err := valueNode.Encode(val); err != nil {
			return nil, err
		}

		row.Content = append(row.Content, keyNode, valueNode)
	}

	return row, nil
}
