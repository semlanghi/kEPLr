package evaluation.esper;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPDeployment;
import org.apache.avro.Schema;

import java.io.IOException;

public class EsperW3 extends EsperBase {


    public EsperW3(Configuration configuration, String run) {
        super(configuration, run);
    }

    @Override
    public void deployQueries(ParametricQueries parametricQueries) {
        try {
            EPCompiled compiled = epCompiler.compile(parametricQueries.getW3(), arguments);
            EPDeployment deployment = epRuntime.getDeploymentService().deploy(compiled);
            deploymentIdMap.put("W3", deployment.getDeploymentId());

            epRuntime.getDeploymentService().getStatement(deployment.getDeploymentId(), "prova").addListener(this.dumpingListener);
        } catch (EPDeployException | EPCompileException e) {
            e.printStackTrace();
        }
    }


}
