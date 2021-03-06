package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"golang.org/x/net/http2"
	"golang.org/x/time/rate"
	"google.golang.org/api/cloudresourcemanager/v1"

	"cloud.google.com/go/bigquery"

	"google.golang.org/api/iam/v1"
)

type Roles struct {
	Roles []Role `json:"roles"`
}

type Role struct {
	Name                string   `json:"name"`
	Title               string   `json:"title,omitempty"`
	Description         string   `json:"description,omitempty"`
	Stage               string   `json:"stage,omitempty"`
	Deleted             bool     `json:"deleted,omitempty"`
	Etag                string   `json:"etag,omitempty"`
	IncludedPermissions []string `json:"included_permissions"`
	Region              string   `json:"region"`
}

type Permissions struct {
	Permissions []Permission `json:"permissions"`
}

type Permission struct {
	Name                    string   `json:"name"`
	Title                   string   `json:"title,omitempty"`
	Description             string   `json:"description,omitempty"`
	Stage                   string   `json:"stage,omitempty"`
	ApiDisabled             bool     `json:"apiDisabled,omitempty"`
	CustomRolesSupportLevel string   `json:"customRolesSupportLevel,omitempty"`
	OnlyInPredefinedRoles   bool     `json:"onlyInPredefinedRoles,omitempty"`
	PrimaryPermission       string   `json:"primaryPermission,omitempty"`
	Roles                   []string `json:"roles"`
	Region                  string   `json:"region"`
}

const (
	maxRequestsPerSecond float64 = 50 // "golang.org/x/time/rate" limiter to throttle operations
	burst                int     = 4
	roleTableName                = "roles"
	permissionsTableName         = "permissions"
)

var (
	cmutex = &sync.Mutex{}
	pmutex = &sync.Mutex{}

	organization = flag.String("organization", os.Getenv("ORGANIZATION_ID"), "OrganizationID")

	mode        = flag.String("mode", "default", "Iteration mode: organization|project|default")
	projects    = make([]*cloudresourcemanager.Project, 0)
	bqDataset   = flag.String("bqDataset", os.Getenv("BQ_DATASET"), "BigQuery Dataset to write to")
	bqProjectID = flag.String("bqProjectID", os.Getenv("BQ_PROJECTID"), "Project for the BigQuery Dataset to write to")
	region      = flag.String("region", os.Getenv("REGION"), "Region where IAM roles/permissions query is run")
	permissions = &Permissions{}
	roles       = &Roles{}
	limiter     *rate.Limiter
	ors         *iam.RolesService

	rolesSchema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType, Required: true},
		{Name: "title", Type: bigquery.StringFieldType},
		{Name: "stage", Type: bigquery.StringFieldType},
		{Name: "etag", Type: bigquery.StringFieldType},
		{Name: "deleted", Type: bigquery.BooleanFieldType},
		{Name: "description", Type: bigquery.StringFieldType},
		{Name: "included_permissions", Type: bigquery.StringFieldType, Repeated: true},
		{Name: "region", Type: bigquery.StringFieldType, Required: true},
	}

	permissionsSchema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType, Required: true},
		{Name: "region", Type: bigquery.StringFieldType, Required: true},
		{Name: "title", Type: bigquery.StringFieldType},
		{Name: "description", Type: bigquery.StringFieldType},
		{Name: "stage", Type: bigquery.StringFieldType},
		{Name: "apiDisabled", Type: bigquery.BooleanFieldType},
		{Name: "customRolesSupportLevel", Type: bigquery.StringFieldType},
		{Name: "onlyInPredefinedRoles", Type: bigquery.BooleanFieldType},
		{Name: "primaryPermission", Type: bigquery.StringFieldType},
		{Name: "roles", Type: bigquery.StringFieldType, Repeated: true},
	}
)

func fronthandler(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("/ called for region %s\n", *region)

	ctx := context.Background()
	crmService, err := cloudresourcemanager.NewService(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	iamService, err := iam.NewService(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ors = iam.NewRolesService(iamService)
	ops := iam.NewPermissionsService(iamService)

	limiter = rate.NewLimiter(rate.Limit(maxRequestsPerSecond), burst)

	// First use iam api to get all testable permissions from root hierarchy (i.,e organization)

	nextPageToken := ""
	for {
		ps, err := ops.QueryTestablePermissions(&iam.QueryTestablePermissionsRequest{
			FullResourceName: fmt.Sprintf("//cloudresourcemanager.googleapis.com/organizations/%s", *organization),
			PageToken:        nextPageToken,
		}).Do()
		if err != nil {
			fmt.Printf("Error getting  QueryTestablePermissions %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, sa := range ps.Permissions {
			permissions.Permissions = append(permissions.Permissions, Permission{
				Name:                    sa.Name,
				Title:                   sa.Title,
				Description:             sa.Description,
				Stage:                   sa.Stage,
				ApiDisabled:             sa.ApiDisabled,
				CustomRolesSupportLevel: sa.CustomRolesSupportLevel,
				OnlyInPredefinedRoles:   sa.OnlyInPredefinedRoles,
				PrimaryPermission:       sa.PrimaryPermission,
				Roles:                   []string{},
				Region:                  *region,
			})
		}

		nextPageToken = ps.NextPageToken
		if nextPageToken == "" {
			break
		}
	}

	fmt.Printf("Found [%d] permissions on //cloudresourcemanager.googleapis.com/organizations/<organizationID>  \n", len(permissions.Permissions))

	switch *mode {
	case "organization":
		fmt.Printf("Getting Organization Roles/Permissions\n")

		if *organization == "" {
			fmt.Printf("Error organizationID cannot be null")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		oreq, err := crmService.Organizations.Get(fmt.Sprintf("organizations/%s", *organization)).Do()
		if err != nil {
			fmt.Printf("Error getting crmService  %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Printf("     Organization Name %s\n", oreq.Name)
		*organization = oreq.Name

		parent := fmt.Sprintf(*organization)

		// A) for Organization Roles
		err = generateMap(ctx, parent)
		if err != nil {
			fmt.Printf("Error generatingMap for Organizations  %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	case "project":
		// B) for Project Roles
		fmt.Printf("Getting Project Roles/Permissions\n")

		// TODO: only get projects in the selected organization
		preq := crmService.Projects.List()
		if err := preq.Pages(ctx, func(page *cloudresourcemanager.ListProjectsResponse) error {
			for _, p := range page.Projects {
				if p.LifecycleState == "ACTIVE" {
					projects = append(projects, p)
				}
			}
			return nil
		}); err != nil {
			fmt.Printf("Error Iterating projects, roles: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, p := range projects {
			parent := fmt.Sprintf("projects/%s", p.ProjectId)
			err = generateMap(ctx, parent)
			if err != nil {
				fmt.Printf("Error getting permissions, roles: %v\n", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	default:

		fmt.Printf("Getting Default Roles/Permissions\n")
		parent := ""
		err = generateMap(ctx, parent)
		if err != nil {
			fmt.Printf("Error getting default permissions, roles: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if len(roles.Roles) == 0 {
		roles.Roles = []Role{}
	}
	if len(permissions.Permissions) == 0 {
		permissions.Permissions = []Permission{}
	}

	fmt.Printf("Generating BigQuery output\n")

	bqClient, err := bigquery.NewClient(ctx, *bqProjectID)
	if err != nil {
		fmt.Printf("Error Creating BQ Client %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ds := bqClient.Dataset(*bqDataset)

	rolesTable := ds.Table(roleTableName)
	_, err = rolesTable.Metadata(ctx)
	if err != nil {
		fmt.Printf("Creating new roles table in dataset\n")
		err = rolesTable.Create(ctx, &bigquery.TableMetadata{
			TimePartitioning: &bigquery.TimePartitioning{
				Type: bigquery.DayPartitioningType,
			},
			Schema: rolesSchema,
		})
		if err != nil {
			fmt.Printf("Error Creating Roles Table %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	fmt.Printf("Uploading [%d] Roles from region [%s]\n", len(roles.Roles), *region)

	var rlines []string
	for _, v := range roles.Roles {
		json, err := json.Marshal(v)
		if err != nil {
			fmt.Printf("Error marshalling Roles  %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rlines = append(rlines, string(json))
	}

	dataString := strings.Join(rlines, "\n")
	rolesSource := bigquery.NewReaderSource(strings.NewReader(dataString))

	rolesSource.SourceFormat = bigquery.JSON
	rolesSource.Schema = rolesSchema

	rloader := rolesTable.LoaderFrom(rolesSource)
	rloader.CreateDisposition = bigquery.CreateNever
	rjob, err := rloader.Run(ctx)
	if err != nil {
		fmt.Printf("Error loading Roles RUN to BQ with JobID %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rstatus, err := rjob.Wait(ctx)
	// if err != nil {
	// 	fmt.Printf("Error loading Roles Wait to BQ jobID [%s]  %v\n", rjob.ID(), err)
	// 	http.Error(w, err.Error(), http.StatusInternalServerError)
	// 	return
	// }
	if err != nil {
		fmt.Printf("Error loading Data:\n %v\n", err)
		return
	}
	if err := rstatus.Err(); err != nil {
		// fmt.Printf("Error loading Roles Status to BQ  jobID [%s]  %v\n", rjob.ID(), err)
		// http.Error(w, err.Error(), http.StatusInternalServerError)
		// return
		fmt.Printf("Error Loading Data Status:\n %v\n", err)
		return
	}
	//fmt.Printf("  Done  %t\n", rstatus.Done())

	permissionsTable := ds.Table(permissionsTableName)
	_, err = permissionsTable.Metadata(ctx)
	if err != nil {
		fmt.Printf("Creating new permissions table in dataset\n")
		err = permissionsTable.Create(ctx, &bigquery.TableMetadata{
			TimePartitioning: &bigquery.TimePartitioning{
				Type: bigquery.DayPartitioningType,
			},
			Schema: permissionsSchema,
		})
		if err != nil {
			fmt.Printf("Error Creating Permissions Table %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	fmt.Printf("Uploading [%d] Permissions from region [%s]\n", len(permissions.Permissions), *region)

	var plines []string
	for _, v := range permissions.Permissions {
		json, err := json.Marshal(v)
		if err != nil {
			fmt.Printf("Error marshalling Permissions  %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		plines = append(plines, string(json))
	}

	dataString = strings.Join(plines, "\n")
	permissionsSource := bigquery.NewReaderSource(strings.NewReader(dataString))
	permissionsSource.SourceFormat = bigquery.JSON
	permissionsSource.Schema = permissionsSchema

	ploader := permissionsTable.LoaderFrom(permissionsSource)

	pjob, err := ploader.Run(ctx)
	if err != nil {
		fmt.Printf("Error loading Permissions RUN to BQ %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	pstatus, err := pjob.Wait(ctx)
	if err != nil {
		fmt.Printf("Error loading Permissions to BQ jobID [%s]  %v\n", pjob.ID(), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := pstatus.Err(); err != nil {
		fmt.Printf("Error loading Permissions to BQ BQ jobID [%s]  %v\n", pjob.ID(), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//fmt.Printf("  Done  %t\n", pstatus.Done())

	fmt.Printf("done\n")

	roles.Roles = []Role{}
	permissions.Permissions = []Permission{}

	fmt.Fprint(w, "ok")
}

func healthhandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("heathcheck...")
	fmt.Fprint(w, "ok")
}

func init() {
	flag.Parse()
}

func main() {
	http.HandleFunc("/", fronthandler)
	http.HandleFunc("/_ah/health", healthhandler)

	var server *http.Server
	server = &http.Server{
		Addr: ":8080",
	}
	http2.ConfigureServer(server, &http2.Server{})
	fmt.Println("Starting Server..")
	err := server.ListenAndServe()
	fmt.Printf("Unable to start Server %v", err)
}

func generateMap(ctx context.Context, parent string) error {
	var wg sync.WaitGroup

	oireq := ors.List().Parent(parent)
	if err := oireq.Pages(ctx, func(page *iam.ListRolesResponse) error {
		for _, sa := range page.Roles {
			wg.Add(1)
			go func(ctx context.Context, wg *sync.WaitGroup, sa *iam.Role) {
				defer wg.Done()
				var err error
				if err := limiter.Wait(ctx); err != nil {
					fmt.Printf("Error in rate limiter %v\n", err)
				}
				if ctx.Err() != nil {
					fmt.Printf("Error in rate limiter %v\n", err)
				}
				rc, err := ors.Get(sa.Name).Do()
				if err != nil {
					fmt.Printf("Error getting role name %v\n", err)
					return
				}
				cr := &Role{
					Name:                sa.Name,
					Title:               sa.Title,
					Description:         sa.Description,
					Stage:               sa.Stage,
					Deleted:             sa.Deleted,
					Etag:                sa.Etag,
					IncludedPermissions: rc.IncludedPermissions,
					Region:              *region,
				}
				cmutex.Lock()
				_, ok := find(roles.Roles, sa.Name)
				if !ok {
					//fmt.Printf("     Iterating Role  %s\n", sa.Name)
					roles.Roles = append(roles.Roles, *cr)
				}
				cmutex.Unlock()
				//fmt.Printf("     Adding Permissions for Role  %s\n", sa.Name)
				for _, perm := range rc.IncludedPermissions {
					//fmt.Printf("     Appending Permission %s to Role %s", perm, sa.Name)
					pmutex.Lock()
					i, ok := find(permissions.Permissions, perm)
					if !ok {
						permissions.Permissions = append(permissions.Permissions, Permission{
							Name:   perm,
							Roles:  []string{sa.Name},
							Region: *region,
						})
					} else {
						p := permissions.Permissions[i]
						cmutex.Lock()
						_, ok := find(p.Roles, sa.Name)
						if !ok {
							p.Roles = append(p.Roles, sa.Name)
							permissions.Permissions[i] = p
						}
						cmutex.Unlock()
					}
					pmutex.Unlock()
				}
			}(ctx, &wg, sa)

		}
		return nil
	}); err != nil {
		return err
	}

	wg.Wait()

	return nil
}

// generics....
func find(slice interface{}, val string) (int, bool) {

	switch slice.(type) {
	case []Role:
		for i, item := range slice.([]Role) {
			if item.Name == val {
				return i, true
			}
		}
	case []Permission:
		for i, item := range slice.([]Permission) {
			if item.Name == val {
				return i, true
			}
		}
	case []string:
		for i, item := range slice.([]string) {
			if item == val {
				return i, true
			}
		}
	default:
		return -1, false
	}

	return -1, false
}
