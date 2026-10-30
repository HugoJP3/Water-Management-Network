1. **Planificación y desarrollo de la práctica**

> No tenemos experiencia reciente con Kafka, Docker, Railway, bases de datos y GitHub. Analiza el enunciado de la práctica y guíanos en su desarrollo paso a paso. Ayúdanos a definir la arquitectura, repartir las tareas, establecer el orden de implementación e identificar los requisitos que debemos cumplir.

2. **Diseño de la base de datos**

> Diseña una base de datos para el sistema Water Management Network a partir de los requisitos del enunciado. Define las entidades, atributos, relaciones, claves primarias y foráneas, y genera un diagrama entidad-relación que represente su estructura.

3. **Diseño del protocolo de comunicación**

> Diseña un protocolo de comunicación mediante sockets para permitir la interacción entre los distintos componentes del sistema (Central, Monitor, Engines y operarios). Define los tipos de mensajes, su estructura, los parámetros necesarios y los mecanismos de validación y gestión de errores. Ten en cuenta que debe ser coherente con la arquitectura general de la práctica.

4. **Implementación de la lógica de negocio e integración con Kafka**

> Guíanos en el desarrollo de la lógica de negocio de la Central y de su integración con Apache Kafka. Define las responsabilidades de cada componente, los topics necesarios, los mensajes que deben intercambiarse y cómo gestionar las notificaciones y los cambios de estado. Explica las decisiones técnicas para que podamos implementar y comprender el funcionamiento.

5. **Auditoría y revisión del código**

> Realiza una auditoría de los códigos que te adjuntamos, buscando errores funcionales, problemas de integración, inconsistencias entre módulos y posibles mejoras. Comprueba especialmente que se respeten los protocolos de comunicación, que las operaciones sobre la base de datos sean coherentes y que los distintos componentes funcionen correctamente entre sí. Justifica los cambios propuestos.

6. **Integración, depuración y pruebas del sistema**

> Estamos integrando los distintos componentes del sistema y hemos encontrado errores. Analiza los códigos, trazas de ejecución y logs que te proporcionamos para identificar el origen de los problemas. Propón correcciones justificadas y establece pruebas funcionales para verificar las comunicaciones, el registro de operarios, las solicitudes de riego, la detección de fugas y la recuperación ante reinicios.

7. **Diseño del panel web de la Central**

> Diseña un panel web para la Central a partir de la información disponible en la base de datos y de la lógica de negocio implementada. Debe permitir visualizar el estado de las estaciones, los riegos y los operarios, además de ofrecer las funcionalidades de gestión necesarias. Debe integrarse con el backend existente y ser compatible con el despliegue en Railway.

8. **Configuración y despliegue de la arquitectura**

> Guíanos en la configuración y el despliegue del sistema mediante Docker y Docker Compose, y en su posterior publicación en Railway. Explica cómo configurar los contenedores, las dependencias, las variables de entorno, los puertos, las direcciones de conexión y la persistencia de datos. Si Railway no está disponible, analiza alternativas de despliegue en AWS EC2 teniendo en cuenta las limitaciones de recursos y costes.

9. **Documentación y automatización de la ejecución**

> Revisa la documentación del repositorio y propón mejoras para que la instalación, configuración y ejecución de la práctica sean claras. Documenta cómo lanzar los distintos componentes desde diferentes máquinas y revisa los scripts de automatización para facilitar las pruebas y la ejecución del sistema.

10. **UML**

> Realiza un UML que represente los códigos que te proporcionamos y sus dependencias.