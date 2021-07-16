package clustering

import (
	"reflect"
	"testing"

	"github.com/TheWeatherCompany/icm-redis-operator/pkg/redis"
)

func TestReplacePrimaries(t *testing.T) {
	node1 := &redis.Node{ID: "node1"}
	node2 := &redis.Node{ID: "node2"}
	node3 := &redis.Node{ID: "node3"}

	newNode1 := &redis.Node{ID: "newNode1"}

	type args struct {
		oldPrimaries       redis.Nodes
		newPrimaries       redis.Nodes
		newNodesNoRole     redis.Nodes
		nbPrimary          int32
		nbPrimaryToReplace int32
	}
	tests := []struct {
		name    string
		args    args
		want    redis.Nodes
		want2   redis.Nodes
		wantErr bool
	}{
		{
			name: "empty slices",
			args: args{
				oldPrimaries:       redis.Nodes{},
				newPrimaries:       redis.Nodes{},
				newNodesNoRole:     redis.Nodes{},
				nbPrimary:          0,
				nbPrimaryToReplace: 0,
			},
			want:    redis.Nodes{},
			want2:   redis.Nodes{},
			wantErr: false,
		},
		{
			name: "no primary to replace",
			args: args{
				oldPrimaries:       redis.Nodes{node1, node2, node3},
				newPrimaries:       redis.Nodes{},
				newNodesNoRole:     redis.Nodes{},
				nbPrimary:          3,
				nbPrimaryToReplace: 0,
			},
			want:    redis.Nodes{node1, node2, node3},
			want2:   redis.Nodes{},
			wantErr: false,
		},
		{
			name: "one primary to replace",
			args: args{
				oldPrimaries:       redis.Nodes{node1, node2, node3},
				newPrimaries:       redis.Nodes{},
				newNodesNoRole:     redis.Nodes{newNode1},
				nbPrimary:          3,
				nbPrimaryToReplace: 1,
			},
			want:    redis.Nodes{node1, node2, newNode1},
			want2:   redis.Nodes{newNode1},
			wantErr: false,
		},
		{
			name: "one primary to replace, current primary already migrated",
			args: args{
				oldPrimaries:       redis.Nodes{node1, node2},
				newPrimaries:       redis.Nodes{node3},
				newNodesNoRole:     redis.Nodes{newNode1},
				nbPrimary:          3,
				nbPrimaryToReplace: 1,
			},
			want:    redis.Nodes{node1, node3, newNode1},
			want2:   redis.Nodes{newNode1},
			wantErr: false,
		},
		{
			name: "not enough new nodes",
			args: args{
				oldPrimaries:       redis.Nodes{node1, node2, node3},
				newPrimaries:       redis.Nodes{},
				newNodesNoRole:     redis.Nodes{newNode1},
				nbPrimary:          3,
				nbPrimaryToReplace: 2,
			},
			want:    redis.Nodes{node1, node2, newNode1},
			want2:   redis.Nodes{newNode1},
			wantErr: true,
		},
		{
			name: "not enough primaries",
			args: args{
				oldPrimaries:       redis.Nodes{node1, node2, node3},
				newPrimaries:       redis.Nodes{},
				newNodesNoRole:     redis.Nodes{newNode1},
				nbPrimary:          5,
				nbPrimaryToReplace: 1,
			},
			want:    redis.Nodes{node1, node2, node3, newNode1},
			want2:   redis.Nodes{newNode1},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got2, err := SelectPrimariesToReplace(tt.args.oldPrimaries, tt.args.newPrimaries, tt.args.newNodesNoRole, tt.args.nbPrimary, tt.args.nbPrimaryToReplace)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReplacePrimaries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sortedGot := got.SortNodes()
			sortedGot2 := got2.SortNodes()
			sortedWant := tt.want.SortNodes()
			sortedWant2 := tt.want2.SortNodes()
			if !reflect.DeepEqual(sortedGot, sortedWant) {
				t.Errorf("ReplacePrimaries().selectedPrimaries = %v, want %v", sortedGot, sortedWant)
			}
			if !reflect.DeepEqual(sortedGot2, sortedWant2) {
				t.Errorf("ReplacePrimaries().newSelectedPrimaries = %v, want %v", sortedGot2, sortedWant2)
			}
		})
	}
}
