package handlers

import catalogv1 "github.com/razvanmarinn/datalake/protobuf/gen/go/catalog/v1"

func CheckProjectExists(client catalogv1.CatalogServiceClient, projectName string) (bool, error) {
	return true, nil
}
