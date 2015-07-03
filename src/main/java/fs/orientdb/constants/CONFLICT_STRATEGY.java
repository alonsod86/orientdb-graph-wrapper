package fs.orientdb.constants;

/**
 * Enums the conflict strategies provided by orientdb when operating with old records
 * @author dgutierrez
 *
 */
public enum CONFLICT_STRATEGY {
	/**  merges the changes */
	AUTOMERGE,
	/**  the default, throw an exception when versions are different */
	VERSION, 
	/** in case the version is different checks if the content is changed, otherwise use the highest version and avoid throwing exception */
	CONTENT
}
